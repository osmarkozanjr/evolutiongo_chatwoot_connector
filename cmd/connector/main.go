// evolution-chatwoot-connector — conector standalone entre evolutiongo e Chatwoot.
//
// Fluxos:
//   - Ingest:  RabbitMQ (filas globais do evolutiongo) -> API do Chatwoot
//   - Egress:  webhook do inbox Chatwoot -> POST /send/* do evolutiongo
//   - Painel:  UI + API de configuração por instância (estilo integração antiga)
//
// A composição final (wire-up de ingest/egress/panel) é concluída na fase de
// integração após os módulos existirem.
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/iceasa/evolution-chatwoot-connector/internal/config"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	app, err := buildApp(ctx, cfg)
	if err != nil {
		log.Fatalf("bootstrap: %v", err)
	}

	if err := app.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
	log.Println("connector encerrado")
}
