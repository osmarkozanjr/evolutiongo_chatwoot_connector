// app.go — composição (wire-up) do conector: store, clients, ingest, egress,
// painel e servidor HTTP. Mantido pelo orquestrador.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/iceasa/evolution-chatwoot-connector/internal/chatwoot"
	"github.com/iceasa/evolution-chatwoot-connector/internal/config"
	"github.com/iceasa/evolution-chatwoot-connector/internal/egress"
	"github.com/iceasa/evolution-chatwoot-connector/internal/evolution"
	"github.com/iceasa/evolution-chatwoot-connector/internal/ingest"
	"github.com/iceasa/evolution-chatwoot-connector/internal/panel"
	"github.com/iceasa/evolution-chatwoot-connector/internal/store"
	"github.com/iceasa/evolution-chatwoot-connector/internal/transport"
)

type app struct {
	cfg      *config.Config
	log      *slog.Logger
	st       store.Store
	consumer transport.Consumer
	server   *http.Server
}

func buildApp(ctx context.Context, cfg *config.Config) (*app, error) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	st, err := store.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	if err := st.Migrate(ctx); err != nil {
		st.Close()
		return nil, fmt.Errorf("migrations: %w", err)
	}

	locker, err := store.NewLocker(cfg.RedisURL)
	if err != nil {
		// Locker é opcional (no-op sem Redis); loga e segue.
		logger.Warn("redis indisponível, usando locker local", "error", err)
	}

	cw := chatwoot.NewHTTPClient()
	evo := evolution.NewHTTPClient(cfg.EvolutionBaseURL)

	// Ingest: eventos do evolutiongo via RabbitMQ (filas globais).
	ingestSvc := ingest.New(st, cw, evo, locker, logger)
	consumer := transport.NewRabbitMQConsumer(cfg.AMQPURL, transport.WithLogger(logger))
	ingestSvc.Register(consumer)

	// Egress: webhook do Chatwoot → /send/* do evolutiongo. O token da
	// instância é resolvido via GET /instance/info/{id} (admin) com cache.
	tokens := newTokenResolver(cfg, logger)
	egressSvc := egress.NewService(st, cw, evo, tokens.resolve, logger)

	// HTTP: painel + API de config + webhook do Chatwoot + health.
	gin.SetMode(gin.ReleaseMode)
	eng := gin.New()
	eng.Use(gin.Recovery())

	// GET e HEAD: healthcheckers como `wget --spider` usam HEAD, e o gin não
	// atende HEAD em rotas registradas só como GET (causava unhealthy no Swarm).
	healthHandler := func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	}
	eng.GET("/health", healthHandler)
	eng.HEAD("/health", healthHandler)
	eng.POST("/chatwoot/webhook/:instanceId", egress.Handler(egressSvc))

	panel.RegisterUI(eng)
	panel.NewHandler(st, cw, cfg).Register(eng)

	srv := &http.Server{
		Addr:              ":" + cfg.ServerPort,
		Handler:           eng,
		ReadHeaderTimeout: 10 * time.Second,
	}

	return &app{cfg: cfg, log: logger, st: st, consumer: consumer, server: srv}, nil
}

// Run sobe o consumer AMQP e o servidor HTTP; encerra os dois quando o ctx
// for cancelado (SIGINT/SIGTERM).
func (a *app) Run(ctx context.Context) error {
	errCh := make(chan error, 2)

	go func() {
		a.log.Info("consumer rabbitmq iniciando", "queues", ingest.Queues())
		err := a.consumer.Start(ctx)
		if ctx.Err() != nil {
			errCh <- nil // shutdown pedido: retorno esperado
			return
		}
		if err == nil || errors.Is(err, context.Canceled) {
			// Retornar sem ctx cancelado é anormal: reporta como falha para o
			// processo sair com exit != 0 e o Swarm (on-failure) reiniciar.
			err = errors.New("consumer: encerrou inesperadamente sem erro")
		}
		errCh <- fmt.Errorf("consumer: %w", err)
	}()

	go func() {
		a.log.Info("http iniciando", "addr", a.server.Addr)
		err := a.server.ListenAndServe()
		if ctx.Err() != nil || errors.Is(err, http.ErrServerClosed) {
			errCh <- nil
			return
		}
		if err == nil {
			err = errors.New("http: encerrou inesperadamente sem erro")
		}
		errCh <- fmt.Errorf("http: %w", err)
	}()

	var firstErr error
	select {
	case <-ctx.Done():
		a.log.Info("shutdown solicitado (SIGTERM/SIGINT)")
	case firstErr = <-errCh:
		if firstErr != nil {
			a.log.Error("componente caiu, encerrando", "error", firstErr)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = a.server.Shutdown(shutdownCtx)
	_ = a.consumer.Close()
	a.st.Close()

	return firstErr
}

// tokenResolver busca o token de uma instância no evolutiongo
// (GET /instance/info/{id}, header apikey = GLOBAL_API_KEY — verificado em
// evolutiongo_original/pkg/routes/routes.go e instance_handler.go: resposta
// {"message":"success","data":{... "token": "..."}}), com cache em memória.
type tokenResolver struct {
	cfg   *config.Config
	log   *slog.Logger
	http  *http.Client
	mu    sync.Mutex
	cache map[string]tokenEntry
}

type tokenEntry struct {
	token   string
	expires time.Time
}

func newTokenResolver(cfg *config.Config, log *slog.Logger) *tokenResolver {
	return &tokenResolver{
		cfg:   cfg,
		log:   log,
		http:  &http.Client{Timeout: 15 * time.Second},
		cache: map[string]tokenEntry{},
	}
}

func (t *tokenResolver) resolve(ctx context.Context, instanceID string) (string, error) {
	t.mu.Lock()
	if e, ok := t.cache[instanceID]; ok && time.Now().Before(e.expires) {
		t.mu.Unlock()
		return e.token, nil
	}
	t.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		t.cfg.EvolutionBaseURL+"/instance/info/"+instanceID, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("apikey", t.cfg.EvolutionAPIKey)

	res, err := t.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("token resolver: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token resolver: /instance/info/%s => HTTP %d", instanceID, res.StatusCode)
	}

	var body struct {
		Data struct {
			Token string `json:"token"`
		} `json:"data"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("token resolver: resposta inválida: %w", err)
	}
	if body.Data.Token == "" {
		return "", fmt.Errorf("token resolver: instância %s sem token na resposta", instanceID)
	}

	t.mu.Lock()
	t.cache[instanceID] = tokenEntry{token: body.Data.Token, expires: time.Now().Add(10 * time.Minute)}
	t.mu.Unlock()
	return body.Data.Token, nil
}
