# Deploy — evolution-chatwoot-connector

Este diretório contém tudo que é necessário para publicar o conector junto da
stack de produção já existente (Swarm + Traefik).

## Visão geral da arquitetura

O grande triunfo deste conector é que ele consegue injetar um botão dentro do
próprio painel do seu evolution go, deixando tudo muito mais simples e
concentrado.

A conexão entre chatwoot e evolution go acontece via RabbitMQ para que nenhuma
mensagem seja perdida.

Na Stack está embutido o service com a imagem do evolution GO, o RabbitMQ e o
connector.

```
                         ┌─────────────────────┐
                         │       Chatwoot       │
                         │  (inbox API Channel) │
                         └──────────┬───────────┘
                                    │  webhook (message_created, ...)
                                    │  api_access_token
                                    ▼
   ┌────────────────────────────────────────────────────────────┐
   │              evolution-chatwoot-connector (:4500)            │
   │  ┌───────────┐   ┌──────────┐   ┌────────────┐   ┌────────┐ │
   │  │  painel   │   │  egress  │   │   ingest   │   │  store │ │
   │  │  (UI/API) │   │ (→ WA)   │   │ (WA → CW)  │   │  PG/RD │ │
   │  └───────────┘   └────┬─────┘   └─────┬──────┘   └────────┘ │
   └────────────────────────┼───────────────┼──────────────────┘
                             │ POST /send/*         ▲ consumo filas globais
                             │ apikey da instância   │ (message, sendmessage,
                             ▼                        │  connection, qrcode,
                  ┌─────────────────────┐             │  contact, historysync)
                  │   evolutiongo API    │             │
                  │      (:4000)         │             │
                  └──────────┬───────────┘             │
                              │ AMQP_GLOBAL_ENABLED     │
                              ▼                          │
                     ┌─────────────────┐                │
                     │    RabbitMQ      │────────────────┘
                     │   (filas globais) │
                     └─────────────────┘
```

- **evolutiongo** publica eventos do WhatsApp em filas globais do RabbitMQ
  (quando `AMQP_GLOBAL_ENABLED=true`) e expõe a API de envio (`/send/*`).
- **RabbitMQ** é o broker novo, adicionado só para o conector — não substitui
  nada da stack atual.
- **evolution-chatwoot-connector** consome as filas globais (ingest), replica
  as mensagens no Chatwoot via API REST, recebe webhooks do inbox do Chatwoot
  e reenvia para o evolutiongo (egress), e expõe um painel web para cadastrar
  instâncias.
- **Chatwoot** só precisa de um inbox do tipo API apontando para o conector;
  o conector registra o webhook do inbox automaticamente ao salvar a
  configuração da instância (ver seção abaixo).

A imagem oficial `evoapicloud/evolution-go:latest` continua sendo usada como
está — **não há fork nem alteração no binário do evolutiongo**. Updates da
imagem upstream continuam funcionando normalmente; a única mudança no service
`evolution_go` é habilitar o AMQP global (variáveis `AMQP_URL`,
`AMQP_GLOBAL_ENABLED`, `AMQP_GLOBAL_EVENTS`), que já existem no binário atual.

## Passo a passo de deploy (Docker Swarm)

### 1. Build da imagem do conector

```bash
cd connector
./deploy/build.sh            # builda iceasa/evolution-chatwoot-connector:latest
# ou com tag explícita:
./deploy/build.sh v0.1.0
# opcional: também publicar no registry (requer docker login):
PUSH=1 ./deploy/build.sh latest
```

O script builda `deploy/Dockerfile` (multi-stage, `golang:1.25-alpine` →
`alpine:3.19`) **sempre com `--no-cache`**, garantindo que a imagem saia com
o código mais recente. É o equivalente a rodar manualmente:

```bash
cd connector
docker build --no-cache -f deploy/Dockerfile -t iceasa/evolution-chatwoot-connector:latest .
```

#### Atualizando o service após o build


**Via terminal:** primeiro execute `docker service ls` para descobrir o nome
do service do conector (procure pelo service com a imagem
`iceasa/evolution-chatwoot-connector`), e então force a atualização:

```bash
docker service ls
docker service update --force --image iceasa/evolution-chatwoot-connector:latest <NOME-DO-SERVICE>
```

### 2. Criar os bancos e volumes necessários

No Postgres já existente na infra (mesmo padrão usado pelo evolutiongo):

```bash
psql -U postgres -c "CREATE DATABASE evogo_chatwoot;"
```

Volume externo do RabbitMQ (Swarm não cria volumes `external: true`
automaticamente):

```bash
docker volume create rabbitmq_data
```

Os volumes `evolution_go_data` e `evolution_go_logs` já existem (stack
atual) e não precisam ser recriados.

### 3. Editar os placeholders do stack file

Abra `connector/deploy/evolution-go-with-connector.yaml` e troque:

| Placeholder | Onde | O que é |
|---|---|---|
| `SEU-TOKEN-AQUI` | `evolution_go.GLOBAL_API_KEY` e `evolution_connector.EVOLUTION_GLOBAL_API_KEY` | mesma chave administrativa do evolutiongo (as duas devem ser **idênticas** — o conector usa essa chave para chamar rotas administrativas do evolutiongo) |
| `SEU-TOKEN-CONECTOR-AQUI` | `evolution_connector.CONNECTOR_API_KEY` | chave própria do conector, protege o painel web e a API de configuração |
| `SENHAAQUI` | DSNs do Postgres | senha do usuário `postgres` (já em uso pelo evolution_go) |
| `SENHA-RABBITMQ-AQUI` | `rabbitmq.RABBITMQ_DEFAULT_PASS`, `AMQP_URL`, `CONNECTOR_AMQP_URL` | senha do usuário `admin` do RabbitMQ (defina uma senha forte e use a MESMA em todas as ocorrências) |

### 4. Criar os configs do injetor de menu (uma vez, antes do deploy)

O stack file usa **configs externos** do Swarm para o injetor do menu
"Chatwoot" na dashboard do evolutiongo (`inject-menu.sh` +
`connector-menu.js`). Eles precisam ser criados **uma vez no servidor
(nó manager), ANTES do deploy** — obrigatório principalmente para deploy
via Portainer, que não tem acesso aos arquivos locais do repositório:

```bash
docker config create inject_menu_sh       connector/deploy/inject-menu.sh
docker config create connector_menu_js connector/deploy/connector-menu.js
```

#### Atualizando o injetor quando o código mudar (garantia em caso de update)

Configs do Swarm são **imutáveis** — não dá para sobrescrever o conteúdo de
um config existente, e o Docker **bloqueia o `docker config rm` enquanto o
config estiver em uso** por um service. Por isso, se `inject-menu.sh` ou
`connector-menu.js` forem atualizados no repositório, **é preciso parar
(remover) o service que os usa antes** de recriar os configs:

> ⚠️ Este procedimento derruba o evolutiongo por alguns instantes, até o
> Update the stack recriar o service.

1. Remova o service que usa os configs (o `evolution_go`; se você não
   alterou o nome no yaml fornecido, ele aparece como
   `evolution_go_evolution_go` no `docker service ls`):

   ```bash
   docker service rm evolution_go_evolution_go
   ```

2. Remova e recrie os configs **com os mesmos nomes**, a partir dos
   arquivos atualizados:

   ```bash
   docker config rm inject_menu_sh connector_menu_js
   docker config create inject_menu_sh       connector/deploy/inject-menu.sh
   docker config create connector_menu_js connector/deploy/connector-menu.js
   ```

3. Faça o **Update the stack** para recriar o service (no Portainer: stack
   → **Editor** → role até o final → marque **"Prune services"** →
   **"Update the stack"**; via terminal:
   `docker stack deploy -c connector/deploy/evolution-go-with-connector.yaml evolution`).

Como o injetor roda **a cada start do container** do `evolution_go`,
atualizações da imagem oficial do evolutiongo são re-patchadas
automaticamente — este procedimento só é necessário quando o **conteúdo**
dos arquivos do injetor (`inject-menu.sh`/`connector-menu.js`) mudar.

### 5. Deploy da stack

**Via Portainer:** vá em **Stacks → Add stack** (ou selecione a stack
existente e clique em **Editor**, se ela já foi criada), cole o conteúdo de
`connector/deploy/evolution-go-with-connector.yaml` (já com os placeholders
preenchidos), role a tela até o final, garanta que a chave **"Prune
services"** esteja marcada e clique em **"Deploy the stack"** (ou **"Update
the stack"**, no caso de stack existente).

**Via terminal:**

```bash
docker stack deploy -c connector/deploy/evolution-go-with-connector.yaml evolution
```

(ajuste o nome da stack conforme já usado em produção — este arquivo é a
evolução do `evolution-go.yaml` atual, então o nome da stack normalmente se
mantém o mesmo).

Verifique os serviços — primeiro liste para descobrir os nomes reais
(variam com o nome da stack):

```bash
docker service ls
docker service logs <NOME-DO-SERVICE-RABBITMQ>
docker service logs <NOME-DO-SERVICE-CONNECTOR>
```

> **Adendo:** se você não alterou o nome do service no yaml fornecido, o
> nome que deve aparecer na coluna `NAME` desejado será
> `evolution_go_evolution_connector`.

No Portainer, o equivalente é **Services** (ou a aba da stack) → clicar no
service → **Service logs**.

## Configurando uma instância no painel

1. No painel do **evolution go** (`https://evolutiongo.iceasa.com.br`),
   clique nas **configurações da instância já criada** (ícone de
   **engrenagem**) da instância desejada.
2. Ao abrir a instância, se tudo ocorreu certo no deploy, o connector terá
   **injetado uma opção escrita "Chatwoot"** na sidebar do painel da
   evolution — e outra no final da tela.
3. Ao clicar nesse menu **pela primeira vez**, será solicitada a
   `CONNECTOR_API_KEY` (a mesma definida no stack file). A partir deste
   momento, a tela exibida **em uma nova janela** pertence ao connector.
4. Repare que o **instance ID já foi capturado** automaticamente. Preencha
   os dados referentes ao Chatwoot:
   - a URL do Chatwoot e o **Token de Acesso (Chatwoot)** — o
     `api_access_token` de um usuário administrador (Configurações de
     Perfil → Access Token), não o token do inbox;
   - o `accountId` do Chatwoot onde o inbox deve ser criado (o número em
     `/app/accounts/<id>` na URL do Chatwoot).
5. **Webhook URL** (campo no final da página do connector): copie esse
   valor e cole na configuração de uma **caixa de mensagens (inbox) já
   existente** no Chatwoot (Configurações → Caixas de Mensagens → sua
   inbox → Configuração → Webhook URL — caso esteja aproveitando
   configurações antigas do Chatwoot). Se a caixa de mensagens for
   **nova**, não é preciso colar nada: marque a opção avançada
   **"Auto Create Inbox"** no formulário do connector, que a inbox é
   criada com o webhook já registrado.
6. Clique em **Salvar** para concluir a configuração. Ao salvar, o conector:
   - cria (ou reaproveita) o inbox do tipo API no Chatwoot;
   - registra o **webhook do inbox automaticamente**, apontando para
     `CONNECTOR_PUBLIC_URL` (`https://evolutiongo.iceasa.com.br/connector`) — não é
     preciso configurar isso manualmente na UI do Chatwoot;
   - passa a consumir os eventos dessa instância vindos das filas globais do
     RabbitMQ e a rotear as respostas do Chatwoot de volta pela API do
     evolutiongo.

## Troubleshooting

**Fila do RabbitMQ sem consumo / mensagens acumulando**
- Confirme que o service `evolution_connector` está rodando e saudável:
  `docker service ps evolution_evolution_connector`.
- Confira `CONNECTOR_AMQP_URL` no conector e `AMQP_URL` no evolutiongo — o
  usuário/senha e o host (`rabbitmq`) precisam ser idênticos.
- Verifique se `AMQP_GLOBAL_ENABLED=true` e se `AMQP_GLOBAL_EVENTS` contém os
  grupos esperados (`MESSAGE,SEND_MESSAGE,CONNECTION,QRCODE,CONTACT,HISTORY_SYNC`)
  — sem isso o evolutiongo nunca publica nas filas globais.
- Acesse a management UI do RabbitMQ (via `docker exec` num container na
  mesma rede, ou habilite temporariamente o Traefik comentado no stack file)
  para inspecionar filas e consumidores.

**401 / apikey inválida**
- Erros do evolutiongo (`/send/*`, rotas administrativas): confira se
  `EVOLUTION_GLOBAL_API_KEY` do conector é **exatamente** igual ao
  `GLOBAL_API_KEY` do `evolution_go`.
- Erros do Chatwoot: confira o `api_access_token` cadastrado na instância no
  painel do conector (token de usuário/agente, não o token do inbox).
- Erros ao acessar o painel/API do conector: confira o header de auth contra
  `CONNECTOR_API_KEY`.

**Webhook do Chatwoot não chega no conector**
- Confirme que `CONNECTOR_PUBLIC_URL` está correto e publicamente acessível
  (`https://evolutiongo.iceasa.com.br/connector/...`) — é essa URL que é registrada no
  inbox do Chatwoot.
- Verifique o roteamento do Traefik: `docker service logs traefik` e as
  labels do service `evolution_connector` no stack file.
- Confirme no admin do Chatwoot (Inbox → Settings → Webhook) se a URL
  cadastrada bate com `CONNECTOR_PUBLIC_URL`; se ela foi alterada depois do
  cadastro inicial da instância, resalve a configuração da instância no
  painel para o conector reemitir o registro do webhook.
- Teste conectividade manual: `curl -i https://evolutiongo.iceasa.com.br/connector/health`.

## Nota sobre a imagem do evolutiongo

O service `evolution_go` deste stack file usa a mesma imagem oficial
`evoapicloud/evolution-go:latest` do `evolution-go.yaml` original, sem
nenhuma modificação de build — apenas variáveis de ambiente novas (AMQP)
foram adicionadas. Atualizações da imagem upstream (`docker service update
--image ...`) continuam funcionando normalmente, sem qualquer dependência do
conector.

## Atalho na dashboard do evolutiongo (sem fork)

O painel do conector é acessível pelo **mesmo domínio** do evolutiongo, em
`https://evolutiongo.iceasa.com.br/connector/` (roteamento por caminho no
Traefik — nenhum subdomínio novo é necessário).

Além disso, o service `evolution_go` usa um entrypoint com patch
(`inject-menu.sh`, montado via Swarm `configs`) que, **a cada start do
container**:

1. copia `connector-menu.js` para `manager/dist/assets/`;
2. injeta a tag `<script>` no `index.html` do manager (idempotente);
3. executa o servidor original (`/app/server`).

Como o manager é servido do filesystem (não é embutido no binário — ver
`pkg/routes/routes.go`), o patch funciona em qualquer versão da imagem
oficial: **toda atualização da imagem é re-patchada automaticamente**. O JS
tenta inserir um item "Chatwoot" na sidebar do manager; se o layout do
upstream mudar, cai para um botão flutuante — nunca quebra a dashboard.

Para desativar o atalho, basta remover `entrypoint:` e `configs:` do service
`evolution_go` no yaml.
