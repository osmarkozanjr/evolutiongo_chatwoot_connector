// Package transport define o consumo de eventos do evolutiongo.
// Implementação RabbitMQ em rabbitmq.go (Agente A). A interface permite
// trocar por NATS no futuro sem tocar no ingest.
package transport

import (
	"context"

	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
)

// Handler processa um evento. Retornar erro => nack/requeue (com limite),
// nil => ack.
type Handler func(ctx context.Context, ev *model.EventEnvelope) error

// Consumer consome eventos das filas globais do evolutiongo.
// Filas globais (AMQP_SPECIFIC_EVENTS/AMQP_GLOBAL_EVENTS) são nomeadas pelo
// evento em minúsculas: message, sendmessage, receipt, connected, disconnected,
// loggedout, qrcode, qrtimeout, qrsuccess, contact, pushname, historysync...
// (verificado em evolutiongo_original/pkg/events/rabbitmq/rabbitmq_producer.go,
// filas quorum duráveis, exchange default "").
type Consumer interface {
	// Subscribe registra o handler para uma fila/evento. Deve ser chamado
	// antes de Start.
	Subscribe(queue string, h Handler)
	// Start conecta e consome até o ctx ser cancelado. Reconecta com backoff.
	Start(ctx context.Context) error
	Close() error
}
