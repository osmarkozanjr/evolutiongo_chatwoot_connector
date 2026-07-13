// Package store define a interface de persistência do conector.
// A implementação Postgres vive em postgres.go (Agente C).
package store

import (
	"context"

	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
)

// Store persiste configs e mapeamentos em Postgres (database evogo_chatwoot).
type Store interface {
	// Configs
	GetConfig(ctx context.Context, instanceID string) (*model.ChatwootConfig, error) // nil, nil quando não existe
	SaveConfig(ctx context.Context, cfg *model.ChatwootConfig) error
	DeleteConfig(ctx context.Context, instanceID string) error
	ListEnabledConfigs(ctx context.Context) ([]*model.ChatwootConfig, error)
	// ListConfigs retorna todas as configs, habilitadas ou não (painel).
	ListConfigs(ctx context.Context) ([]*model.ChatwootConfig, error)

	// Contatos
	GetContact(ctx context.Context, instanceID, remoteJid string) (*model.ContactMapping, error)
	SaveContact(ctx context.Context, m *model.ContactMapping) error

	// Conversas
	GetConversation(ctx context.Context, instanceID, remoteJid string) (*model.ConversationMapping, error)
	// GetConversationByChatwootID resolve o mapeamento a partir do lado Chatwoot
	// (webhook de inbox só traz o conversation_id do Chatwoot).
	GetConversationByChatwootID(ctx context.Context, instanceID string, cwConversationID int) (*model.ConversationMapping, error)
	SaveConversation(ctx context.Context, m *model.ConversationMapping) error
	DeleteConversation(ctx context.Context, instanceID, remoteJid string) error

	// Mensagens (dedup + correlação)
	GetMessageByWhatsappID(ctx context.Context, instanceID, waMessageID string) (*model.MessageMapping, error)
	GetMessageByChatwootID(ctx context.Context, instanceID string, cwMessageID int) (*model.MessageMapping, error)
	SaveMessage(ctx context.Context, m *model.MessageMapping) error

	// Migrate aplica as migrations (chamado no startup).
	Migrate(ctx context.Context) error
	Close()
}
