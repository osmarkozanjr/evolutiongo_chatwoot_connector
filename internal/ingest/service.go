// Package ingest implementa o fluxo WA→Chatwoot: consome os eventos globais do
// evolutiongo (via transport.Consumer) e os materializa no Chatwoot
// (contatos, conversas, mensagens, mensagens de bot e import de histórico).
//
// Porta em Go da lógica de eventWhatsapp() em
// evolutionapi_antiga/src/api/integrations/chatbot/chatwoot/services/chatwoot.service.ts,
// adaptada ao formato de eventos do evolutiongo
// (evolutiongo_original/pkg/whatsmeow/service/whatsmeow.go).
//
// Formato do payload (verificado no whatsmeow.go): o campo `data` dos eventos
// Message/SendMessage é o struct events.Message do whatsmeow re-marshalado —
// chaves de topo em PascalCase Go ("Info", "Message", "RawMessage" é removido),
// e o conteúdo de "Message" com as chaves camelCase do protobuf waE2E
// ("conversation", "imageMessage", "extendedTextMessage"...). Como as chaves
// internas de "Info" dependem das tags JSON do whatsmeow (não disponíveis no
// workspace para inspeção), os helpers de parsing aceitam as duas grafias
// (PascalCase e camelCase) — ver TODO(VERIFY) em message.go.
package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/iceasa/evolution-chatwoot-connector/internal/chatwoot"
	"github.com/iceasa/evolution-chatwoot-connector/internal/evolution"
	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
	"github.com/iceasa/evolution-chatwoot-connector/internal/store"
	"github.com/iceasa/evolution-chatwoot-connector/internal/transport"
)

// Queues lista as filas globais do evolutiongo consumidas pelo ingest.
// Nomes = evento em minúsculas (verificado em SendToGlobalQueues:
// amqpQueueName = strings.ToLower(eventType)).
func Queues() []string {
	return []string{
		"message",
		"sendmessage",
		"receipt",
		"connected",
		"disconnected",
		"loggedout",
		"qrcode",
		"qrtimeout",
		"qrsuccess",
		"contact",
		"pushname",
		"historysync",
	}
}

// Service roteia eventos do evolutiongo para o Chatwoot.
type Service struct {
	store  store.Store
	cw     chatwoot.Client
	evo    evolution.Client
	locker store.Locker
	log    *slog.Logger
	httpc  *http.Client

	// throttle de notificação de conexão (porta do lastConnectionNotification
	// do serviço antigo — evita spam de "conectado" na conversa do bot).
	connMu       sync.Mutex
	connNotified map[string]time.Time
}

// New cria o serviço de ingest. locker pode ser nil (vira lock local no-op —
// aceitável em instância única; com múltiplas réplicas use store.NewLocker
// com Redis para serializar a criação de conversas por remoteJid, como fazia
// o lock Redis de createConversation no serviço antigo).
func New(st store.Store, cw chatwoot.Client, evo evolution.Client, locker store.Locker, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	if locker == nil {
		locker, _ = store.NewLocker("") // no-op locker
	}
	return &Service{
		store:        st,
		cw:           cw,
		evo:          evo,
		locker:       locker,
		log:          logger,
		httpc:        &http.Client{Timeout: 2 * time.Minute},
		connNotified: map[string]time.Time{},
	}
}

// Register inscreve o handler do serviço em todas as filas do ingest.
func (s *Service) Register(c transport.Consumer) {
	for _, q := range Queues() {
		c.Subscribe(q, s.Handle)
	}
}

// Handle implementa transport.Handler: carrega a config da instância e roteia
// por evento. Retornar erro => nack/requeue (com limite no transport);
// retornar nil => ack. Payload malformado é descartado (nil) com log — requeue
// não resolveria.
func (s *Service) Handle(ctx context.Context, ev *model.EventEnvelope) error {
	if ev == nil || ev.InstanceID == "" {
		return nil
	}

	cfg, err := s.store.GetConfig(ctx, ev.InstanceID)
	if err != nil {
		return fmt.Errorf("ingest: erro ao carregar config da instância %s: %w", ev.InstanceID, err)
	}
	if cfg == nil || !cfg.Enabled {
		// Instância sem integração habilitada: ignora silenciosamente.
		return nil
	}

	switch strings.ToLower(ev.Event) {
	case "message", "sendmessage":
		return s.handleMessageEvent(ctx, cfg, ev)
	case "receipt":
		return s.handleReceipt(ctx, cfg, ev)
	case "connected", "pairsuccess", "qrsuccess":
		return s.handleConnected(ctx, cfg, ev)
	case "disconnected":
		return s.handleDisconnected(ctx, cfg, ev)
	case "loggedout":
		return s.handleLoggedOut(ctx, cfg, ev)
	case "qrcode":
		return s.handleQRCode(ctx, cfg, ev)
	case "qrtimeout":
		return s.handleQRTimeout(ctx, cfg, ev)
	case "contact":
		return s.handleContactEvent(ctx, cfg, ev)
	case "pushname":
		return s.handlePushNameEvent(ctx, cfg, ev)
	case "historysync":
		return s.handleHistorySync(ctx, cfg, ev)
	default:
		s.log.Debug("ingest: evento não roteado", "event", ev.Event, "instanceId", ev.InstanceID)
		return nil
	}
}

// handleReceipt: o serviço antigo usava messages.read apenas para atualizar o
// "last seen" da conversa via API pública do Chatwoot
// (/public/api/v1/inboxes/.../update_last_seen). A interface chatwoot.Client
// atual não expõe essa operação; o evento é ack'ado sem efeito.
// TODO(VERIFY): se o update_last_seen for desejado, é preciso um método novo
// no chatwoot.Client (ver comentário INTERFACE-CHANGE-REQUEST em message.go).
func (s *Service) handleReceipt(_ context.Context, _ *model.ChatwootConfig, ev *model.EventEnvelope) error {
	s.log.Debug("ingest: receipt ignorado (update_last_seen não suportado)", "instanceId", ev.InstanceID)
	return nil
}

// shouldIgnoreJid porta o filtro ignoreJids de eventWhatsapp():
//   - "@g.us" na lista => ignora todos os grupos;
//   - "@s.whatsapp.net" na lista => ignora todos os contatos diretos;
//   - JID exato na lista => ignora aquele chat.
func shouldIgnoreJid(ignoreJids []string, remoteJid string) bool {
	if remoteJid == "" {
		return false
	}
	for _, ig := range ignoreJids {
		switch {
		case ig == "@g.us" && strings.HasSuffix(remoteJid, "@g.us"):
			return true
		case ig == "@s.whatsapp.net" && strings.HasSuffix(remoteJid, "@s.whatsapp.net"):
			return true
		case ig == remoteJid:
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Helpers de parsing tolerantes a grafia de chave (PascalCase Go x camelCase).
// ---------------------------------------------------------------------------

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// getAny retorna o primeiro valor presente entre as chaves candidatas.
func getAny(m map[string]any, keys ...string) (any, bool) {
	if m == nil {
		return nil, false
	}
	for _, k := range keys {
		if v, ok := m[k]; ok && v != nil {
			return v, true
		}
	}
	return nil, false
}

func getMap(m map[string]any, keys ...string) map[string]any {
	v, ok := getAny(m, keys...)
	if !ok {
		return nil
	}
	return asMap(v)
}

func getSlice(m map[string]any, keys ...string) []any {
	v, ok := getAny(m, keys...)
	if !ok {
		return nil
	}
	s, _ := v.([]any)
	return s
}

func getString(m map[string]any, keys ...string) string {
	v, ok := getAny(m, keys...)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func getBool(m map[string]any, keys ...string) bool {
	v, ok := getAny(m, keys...)
	if !ok {
		return false
	}
	b, _ := v.(bool)
	return b
}

// jidToString normaliza um JID que pode vir como string
// ("5511999999999@s.whatsapp.net") ou como objeto types.JID
// ({User, Server, Device...} ou {user, server, device...}).
// TODO(VERIFY): a grafia exata depende de o types.JID do whatsmeow
// implementar (ou não) TextMarshaler na versão usada pelo evolutiongo
// (v0.0.0-20260630...); o código de referência não deixa isso observável.
func jidToString(v any) string {
	switch j := v.(type) {
	case string:
		return j
	case map[string]any:
		user := getString(j, "User", "user")
		server := getString(j, "Server", "server")
		if user == "" && server == "" {
			return ""
		}
		if server == "" {
			return user
		}
		return user + "@" + server
	default:
		return ""
	}
}

// jidUser extrai a parte numérica de um JID ("5511...@s.whatsapp.net" =>
// "5511..."), removendo sufixo de device (":12").
func jidUser(jid string) string {
	user := jid
	if i := strings.IndexByte(user, '@'); i >= 0 {
		user = user[:i]
	}
	if i := strings.IndexByte(user, ':'); i >= 0 {
		user = user[:i]
	}
	return user
}

func isGroupJid(jid string) bool { return strings.HasSuffix(jid, "@g.us") }

// parseTimestamp aceita RFC3339 (time.Time marshalado pelo Go) ou epoch em
// segundos (número ou string), como aparece em messageTimestamp do histórico.
func parseTimestamp(v any) time.Time {
	switch t := v.(type) {
	case string:
		if ts, err := time.Parse(time.RFC3339, t); err == nil {
			return ts
		}
		if ts, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return ts
		}
		var epoch int64
		if _, err := fmt.Sscanf(t, "%d", &epoch); err == nil && epoch > 0 {
			return time.Unix(epoch, 0)
		}
	case float64:
		if t > 0 {
			return time.Unix(int64(t), 0)
		}
	}
	return time.Time{}
}
