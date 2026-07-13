// rabbitmq.go — implementação RabbitMQ de transport.Consumer (Agente A).
//
// Consome as filas globais do evolutiongo. Contratos verificados em
// evolutiongo_original/pkg/events/rabbitmq/rabbitmq_producer.go:
//   - exchange default "" (routing key = nome da fila);
//   - filas quorum duráveis: x-queue-type=quorum, x-ha-policy=all, durable=true,
//     autoDelete=false, exclusive=false, noWait=false — o consumer declara com
//     EXATAMENTE os mesmos argumentos para não colidir com o producer;
//   - mensagens persistentes, content-type application/json.
//
// Política de retry: handler retornou erro => nack+requeue. Filas quorum
// incrementam o header "x-delivery-count" a cada redelivery; quando o número
// de tentativas atinge maxAttempts a mensagem é ack'ada e descartada com log
// (evita loop infinito de redelivery). Fallback: header "x-death" (caso a fila
// tenha DLX configurada externamente).
package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
)

const (
	defaultMaxAttempts  = 5
	defaultPrefetch     = 20
	reconnectBackoffMin = 1 * time.Second
	reconnectBackoffMax = 30 * time.Second
	heartbeatInterval   = 30 * time.Second // igual ao producer do evolutiongo
)

// quorumQueueArgs replica os argumentos usados pelo producer do evolutiongo
// (rabbitmq_producer.go). Declarar com args diferentes causaria
// PRECONDITION_FAILED no broker.
func quorumQueueArgs() amqp.Table {
	return amqp.Table{
		"x-queue-type": "quorum",
		"x-ha-policy":  "all",
	}
}

// RabbitMQConsumer implementa Consumer com reconexão automática e backoff
// exponencial. Subscribe deve ser chamado antes de Start.
type RabbitMQConsumer struct {
	url         string
	maxAttempts int
	prefetch    int
	log         *slog.Logger

	mu       sync.Mutex
	handlers map[string]Handler
	conn     *amqp.Connection
	closed   bool
}

var _ Consumer = (*RabbitMQConsumer)(nil)

// RabbitMQOption configura o consumer.
type RabbitMQOption func(*RabbitMQConsumer)

// WithMaxAttempts define o número máximo de tentativas de entrega antes do
// descarte (default 5).
func WithMaxAttempts(n int) RabbitMQOption {
	return func(c *RabbitMQConsumer) {
		if n > 0 {
			c.maxAttempts = n
		}
	}
}

// WithPrefetch define o QoS prefetch por canal (default 20).
func WithPrefetch(n int) RabbitMQOption {
	return func(c *RabbitMQConsumer) {
		if n > 0 {
			c.prefetch = n
		}
	}
}

// WithLogger define o logger (default slog.Default()).
func WithLogger(l *slog.Logger) RabbitMQOption {
	return func(c *RabbitMQConsumer) {
		if l != nil {
			c.log = l
		}
	}
}

// NewRabbitMQConsumer cria o consumer apontando para a URL AMQP
// (ex. amqp://admin:admin@rabbitmq:5672/ — CONNECTOR_AMQP_URL).
func NewRabbitMQConsumer(url string, opts ...RabbitMQOption) *RabbitMQConsumer {
	c := &RabbitMQConsumer{
		url:         url,
		maxAttempts: defaultMaxAttempts,
		prefetch:    defaultPrefetch,
		log:         slog.Default(),
		handlers:    map[string]Handler{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Subscribe registra o handler de uma fila global (ex. "message"). Deve ser
// chamado antes de Start; chamadas posteriores são ignoradas na sessão atual
// e passam a valer na próxima reconexão.
func (c *RabbitMQConsumer) Subscribe(queue string, h Handler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[queue] = h
}

// Start conecta e consome até o ctx ser cancelado ou Close ser chamado.
// Reconecta com backoff exponencial (1s..30s) em caso de queda.
func (c *RabbitMQConsumer) Start(ctx context.Context) error {
	c.mu.Lock()
	n := len(c.handlers)
	c.mu.Unlock()
	if n == 0 {
		return errors.New("transport: nenhuma fila registrada (chame Subscribe antes de Start)")
	}

	backoff := reconnectBackoffMin
	for {
		if ctx.Err() != nil || c.isClosed() {
			return nil
		}

		start := time.Now()
		err := c.runSession(ctx)
		if ctx.Err() != nil || c.isClosed() {
			return nil
		}
		if err != nil {
			c.log.Error("rabbitmq: sessão encerrada, reconectando", "error", err, "backoff", backoff)
		}
		// Sessão que viveu o suficiente reseta o backoff.
		if time.Since(start) > time.Minute {
			backoff = reconnectBackoffMin
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > reconnectBackoffMax {
			backoff = reconnectBackoffMax
		}
	}
}

// Close encerra a conexão e faz Start retornar.
func (c *RabbitMQConsumer) Close() error {
	c.mu.Lock()
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil && !conn.IsClosed() {
		return conn.Close()
	}
	return nil
}

func (c *RabbitMQConsumer) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// runSession abre uma conexão, declara as filas e consome até a conexão cair
// ou o ctx ser cancelado.
func (c *RabbitMQConsumer) runSession(ctx context.Context) error {
	conn, err := amqp.DialConfig(c.url, amqp.Config{
		Heartbeat: heartbeatInterval,
		Locale:    "en_US",
	})
	if err != nil {
		return fmt.Errorf("transport: falha ao conectar no RabbitMQ: %w", err)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = conn.Close()
		return nil
	}
	c.conn = conn
	handlers := make(map[string]Handler, len(c.handlers))
	for q, h := range c.handlers {
		handlers[q] = h
	}
	c.mu.Unlock()

	defer func() {
		if !conn.IsClosed() {
			_ = conn.Close()
		}
	}()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("transport: falha ao abrir canal: %w", err)
	}
	if err := ch.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("transport: falha ao configurar QoS: %w", err)
	}

	var wg sync.WaitGroup
	for queue, handler := range handlers {
		if _, err := ch.QueueDeclare(
			queue, // name
			true,  // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			quorumQueueArgs(),
		); err != nil {
			return fmt.Errorf("transport: falha ao declarar fila %s: %w", queue, err)
		}

		deliveries, err := ch.Consume(
			queue,                              // queue
			"evochatwoot-connector-"+queue,     // consumer tag
			false,                              // auto-ack (fazemos ack manual)
			false,                              // exclusive
			false,                              // no-local
			false,                              // no-wait
			nil,                                // args
		)
		if err != nil {
			return fmt.Errorf("transport: falha ao consumir fila %s: %w", queue, err)
		}

		wg.Add(1)
		go func(queue string, h Handler, deliveries <-chan amqp.Delivery) {
			defer wg.Done()
			for d := range deliveries {
				c.handleDelivery(ctx, queue, h, d)
			}
		}(queue, handler, deliveries)
	}

	c.log.Info("rabbitmq: consumindo filas globais", "queues", len(handlers))

	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
	var sessionErr error
	select {
	case <-ctx.Done():
		_ = conn.Close()
	case amqpErr, ok := <-closeCh:
		if ok && amqpErr != nil {
			sessionErr = fmt.Errorf("transport: conexão fechada pelo broker: %w", amqpErr)
		}
	}
	// Os canais de delivery fecham junto com a conexão.
	wg.Wait()
	return sessionErr
}

// handleDelivery decodifica o envelope e aplica a política ack/nack.
func (c *RabbitMQConsumer) handleDelivery(ctx context.Context, queue string, h Handler, d amqp.Delivery) {
	var ev model.EventEnvelope
	if err := json.Unmarshal(d.Body, &ev); err != nil {
		// Payload que não é um envelope válido nunca vai passar a ser —
		// descarta (ack) para não travar a fila.
		c.log.Warn("rabbitmq: envelope inválido descartado", "queue", queue, "error", err)
		_ = d.Ack(false)
		return
	}

	err := h(ctx, &ev)
	if err == nil {
		_ = d.Ack(false)
		return
	}

	attempt := deliveryAttempt(d)
	if attempt >= c.maxAttempts {
		c.log.Error("rabbitmq: mensagem descartada após esgotar tentativas",
			"queue", queue, "event", ev.Event, "instanceId", ev.InstanceID,
			"attempts", attempt, "error", err)
		_ = d.Ack(false)
		return
	}

	c.log.Warn("rabbitmq: erro no handler, mensagem reenfileirada",
		"queue", queue, "event", ev.Event, "instanceId", ev.InstanceID,
		"attempt", attempt, "max", c.maxAttempts, "error", err)
	// Pequena espera proporcional à tentativa para não redeliverar em loop
	// quente (filas quorum redeliveram imediatamente). Limitada pelo prefetch.
	delay := time.Duration(attempt) * 500 * time.Millisecond
	if delay > 5*time.Second {
		delay = 5 * time.Second
	}
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
	_ = d.Nack(false, true)
}

// deliveryAttempt calcula o número da tentativa atual (1 = primeira entrega).
// Filas quorum expõem "x-delivery-count" = quantidade de redeliveries
// anteriores. Fallback: contagem do header "x-death" (DLX) e o flag Redelivered.
func deliveryAttempt(d amqp.Delivery) int {
	if d.Headers != nil {
		if v, ok := d.Headers["x-delivery-count"]; ok {
			if n, ok := toInt(v); ok {
				return n + 1
			}
		}
		if v, ok := d.Headers["x-death"]; ok {
			if deaths, ok := v.([]interface{}); ok {
				total := 0
				for _, dv := range deaths {
					if dm, ok := dv.(amqp.Table); ok {
						if n, ok := toInt(dm["count"]); ok {
							total += n
						}
					}
				}
				if total > 0 {
					return total + 1
				}
			}
		}
	}
	if d.Redelivered {
		// Sem contador disponível: só sabemos que não é a primeira entrega.
		return 2
	}
	return 1
}

func toInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int8:
		return int(n), true
	case int16:
		return int(n), true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case uint8:
		return int(n), true
	case uint16:
		return int(n), true
	case uint32:
		return int(n), true
	case uint64:
		return int(n), true
	case float32:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}
