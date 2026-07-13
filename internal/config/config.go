// Package config carrega a configuração do conector via variáveis de ambiente.
package config

import (
	"fmt"
	"os"
)

type Config struct {
	// HTTP
	ServerPort string // CONNECTOR_PORT (default 4500)
	PublicURL  string // CONNECTOR_PUBLIC_URL — ex. https://evoconnector.iceasa.com.br (usado no webhook do inbox)
	APIKey     string // CONNECTOR_API_KEY — protege painel e API de config

	// Infra
	PostgresDSN string // CONNECTOR_POSTGRES_DSN — ex. postgresql://postgres:...@postgres:5432/evogo_chatwoot?sslmode=disable
	RedisURL    string // CONNECTOR_REDIS_URL — ex. redis://redis:6379/4
	AMQPURL     string // CONNECTOR_AMQP_URL — ex. amqp://admin:admin@rabbitmq:5672/

	// evolutiongo
	EvolutionBaseURL string // EVOLUTION_BASE_URL — ex. http://evolution_go:4000 (rede interna)
	EvolutionAPIKey  string // EVOLUTION_GLOBAL_API_KEY — GLOBAL_API_KEY do evolutiongo (operações administrativas)
}

func get(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Load lê o ambiente e valida os campos obrigatórios.
func Load() (*Config, error) {
	c := &Config{
		ServerPort:       get("CONNECTOR_PORT", "4500"),
		PublicURL:        os.Getenv("CONNECTOR_PUBLIC_URL"),
		APIKey:           os.Getenv("CONNECTOR_API_KEY"),
		PostgresDSN:      os.Getenv("CONNECTOR_POSTGRES_DSN"),
		RedisURL:         get("CONNECTOR_REDIS_URL", ""),
		AMQPURL:          os.Getenv("CONNECTOR_AMQP_URL"),
		EvolutionBaseURL: get("EVOLUTION_BASE_URL", "http://evolution_go:4000"),
		EvolutionAPIKey:  os.Getenv("EVOLUTION_GLOBAL_API_KEY"),
	}
	var missing []string
	for k, v := range map[string]string{
		"CONNECTOR_PUBLIC_URL":   c.PublicURL,
		"CONNECTOR_API_KEY":      c.APIKey,
		"CONNECTOR_POSTGRES_DSN": c.PostgresDSN,
		"CONNECTOR_AMQP_URL":     c.AMQPURL,
	} {
		if v == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("variáveis de ambiente obrigatórias ausentes: %v", missing)
	}
	return c, nil
}
