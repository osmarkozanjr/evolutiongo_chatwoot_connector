// Package store — helper opcional de lock distribuído via Redis (go-redis/v9).
// Usado, por exemplo, para serializar createConversation/createContact por
// instância+remoteJid (ver CONTRACTS.md item 4: "locks Redis em createConversation"
// na integração antiga). Dono: Agente C.
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Locker adquire e libera locks distribuídos de curta duração.
type Locker interface {
	// AcquireLock tenta adquirir o lock "key" por até ttl. Se adquirido, retorna
	// true e uma função release() que deve ser chamada (idempotente) para
	// liberar o lock antes do TTL expirar. Se não adquirido, retorna
	// (false, nil, nil) — não é erro, apenas "já está travado por outro".
	AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, func(), error)
}

// releaseScript libera o lock apenas se o valor armazenado ainda for o token
// que o adquiriu (evita liberar um lock que já expirou e foi readquirido por
// outro processo — clássico "check-and-delete" atômico via Lua).
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`)

// RedisLocker implementa Locker usando SET NX PX + release via script Lua.
type RedisLocker struct {
	client *redis.Client
}

var _ Locker = (*RedisLocker)(nil)

// NewLocker cria um Locker a partir da URL do Redis (ex. redis://redis:6379/4,
// ver CONNECTOR_REDIS_URL em internal/config).
//
// Construtor tolerante: se redisURL for vazia, retorna um locker no-op que
// sempre adquire o lock (locking desabilitado — útil para rodar o conector
// sem Redis em dev/single-instance, aceitando o risco de condições de corrida
// que o lock existiria para evitar).
func NewLocker(redisURL string) (Locker, error) {
	if redisURL == "" {
		return noopLocker{}, nil
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("store: redis url inválida: %w", err)
	}
	return &RedisLocker{client: redis.NewClient(opts)}, nil
}

// Close encerra a conexão com o Redis.
func (l *RedisLocker) Close() error {
	if l.client != nil {
		return l.client.Close()
	}
	return nil
}

// AcquireLock implementa Locker.
func (l *RedisLocker) AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, func(), error) {
	token, err := randomToken()
	if err != nil {
		return false, nil, fmt.Errorf("store: erro ao gerar token de lock: %w", err)
	}

	ok, err := l.client.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return false, nil, fmt.Errorf("store: erro ao adquirir lock %s: %w", key, err)
	}
	if !ok {
		return false, nil, nil
	}

	release := func() {
		// Contexto próprio: o release pode ocorrer após o ctx original ter
		// sido cancelado (ex. defer release() rodando durante shutdown).
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = releaseScript.Run(releaseCtx, l.client, []string{key}, token).Err()
	}
	return true, release, nil
}

func randomToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// noopLocker sempre adquire o lock com sucesso — usado quando não há Redis
// configurado (ver NewLocker).
type noopLocker struct{}

var _ Locker = noopLocker{}

func (noopLocker) AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, func(), error) {
	return true, func() {}, nil
}
