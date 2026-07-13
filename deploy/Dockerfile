# syntax=docker/dockerfile:1
#
# Dockerfile do evolution-chatwoot-connector.
# Build multi-stage: compila um binário estático em golang:1.25-alpine e
# empacota numa imagem alpine mínima, sem toolchain Go no runtime.
#
# Build (a partir da raiz do módulo, connector/):
#   docker build -f deploy/Dockerfile -t iceasa/evolution-chatwoot-connector:latest .

# ---------- Etapa 1: build ----------
FROM golang:1.25-alpine AS build

# ca-certificates: necessário para o `go mod download` resolver módulos via HTTPS.
RUN apk add --no-cache ca-certificates

WORKDIR /build

# Copia apenas os arquivos de dependência primeiro para aproveitar o cache de
# camadas do Docker — só re-baixa módulos quando go.mod/go.sum mudam.
COPY go.mod go.sum* ./
RUN go mod download

# Copia o restante do código-fonte.
COPY . .

ARG VERSION=dev
# CGO_ENABLED=0: binário estático, sem dependência de libc do runtime.
# O conector usa apenas dependências puro-Go (gin, pgx/v5, amqp091-go, go-redis/v9).
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /build/connector \
    ./cmd/connector

# ---------- Etapa 2: runtime ----------
FROM alpine:3.19 AS final

# ca-certificates: necessário para chamadas HTTPS de saída (API do Chatwoot,
# instâncias evolutiongo expostas via HTTPS, etc).
# wget: usado apenas pelo HEALTHCHECK abaixo (já vem no busybox do alpine, mas
# reinstalamos explicitamente para garantir a variante com suporte a --spider).
# tzdata: timezone correto em logs/timestamps.
RUN apk add --no-cache ca-certificates wget tzdata \
    && addgroup -S connector \
    && adduser -S -G connector -H -h /app connector

ENV TZ=America/Sao_Paulo

WORKDIR /app

COPY --from=build /build/connector /app/connector

RUN chown -R connector:connector /app

USER connector

EXPOSE 4500

# O endpoint /health é exposto pelo wire-up (app.go) do conector.
# GET real (não --spider): --spider faz HEAD, que nem todo framework atende
# em rotas GET — foi a causa de unhealthy+SIGTERM no Swarm (bug corrigido).
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:4500/health || exit 1

ENTRYPOINT ["/app/connector"]
