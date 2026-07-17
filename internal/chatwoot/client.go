// Package chatwoot define o client da API HTTP do Chatwoot.
// A implementação vive em client_impl.go (Agente B).
// Referência: app Rails em chatwoot/ (rotas api/v1) e uso do @figuro/chatwoot-sdk
// na evolutionapi_antiga/src/api/integrations/chatbot/chatwoot/services/chatwoot.service.ts.
package chatwoot

import (
	"context"

	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
)

// Contact é o subconjunto de campos do contato Chatwoot que o conector usa.
type Contact struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Identifier  string `json:"identifier"`
	PhoneNumber string `json:"phone_number"`
	AvatarURL   string `json:"avatar_url"`
}

// Conversation é o subconjunto de campos da conversa Chatwoot que o conector usa.
type Conversation struct {
	ID      int    `json:"id"`
	InboxID int    `json:"inbox_id"`
	Status  string `json:"status"`
}

// Message representa uma mensagem criada no Chatwoot.
type Message struct {
	ID             int    `json:"id"`
	Content        string `json:"content"`
	MessageType    string `json:"message_type"` // incoming | outgoing
	ConversationID int    `json:"conversation_id"`
}

// Attachment descreve um anexo a subir junto com a mensagem.
type Attachment struct {
	Filename string
	Mime     string
	Data     []byte
}

// Client fala com a API do Chatwoot usando cfg.URL, cfg.AccountID e cfg.Token
// (header api_access_token).
type Client interface {
	// Inboxes
	FindInboxByName(ctx context.Context, cfg *model.ChatwootConfig, name string) (inboxID int, err error) // 0 se não existir
	CreateInbox(ctx context.Context, cfg *model.ChatwootConfig, name, webhookURL string) (inboxID int, err error)
	UpdateInboxWebhook(ctx context.Context, cfg *model.ChatwootConfig, inboxID int, webhookURL string) error

	// Contatos
	SearchContact(ctx context.Context, cfg *model.ChatwootConfig, phoneOrIdentifier string) (*Contact, error) // nil,nil se não achou
	CreateContact(ctx context.Context, cfg *model.ChatwootConfig, inboxID int, phone, name, avatarURL, identifier string) (*Contact, error)
	UpdateContact(ctx context.Context, cfg *model.ChatwootConfig, contactID int, fields map[string]any) error
	MergeContacts(ctx context.Context, cfg *model.ChatwootConfig, baseID, mergeID int) error

	// Conversas
	GetOpenConversation(ctx context.Context, cfg *model.ChatwootConfig, contactID, inboxID int) (*Conversation, error) // nil,nil se não achou
	CreateConversation(ctx context.Context, cfg *model.ChatwootConfig, contactID, inboxID int, status string) (*Conversation, error)
	ToggleConversationStatus(ctx context.Context, cfg *model.ChatwootConfig, conversationID int, status string) error

	// Mensagens
	CreateMessage(ctx context.Context, cfg *model.ChatwootConfig, conversationID int, content, messageType string, attachments []Attachment, sourceID string) (*Message, error)
	DeleteMessage(ctx context.Context, cfg *model.ChatwootConfig, conversationID, messageID int) error
}
