// Package model define os tipos compartilhados entre ingest, egress, store e panel.
// NÃO adicionar lógica aqui — apenas tipos e constantes.
package model

import (
	"encoding/json"
	"time"
)

// EventEnvelope é o envelope JSON publicado pelo evolutiongo em cada evento.
// Verificado em evolutiongo_original/pkg/whatsmeow/service/whatsmeow.go:
// postMap = { "event": "...", "data": {...}, "instanceId": "...", "instanceName": "...", "instanceToken": "..." }
type EventEnvelope struct {
	Event         string          `json:"event"`
	InstanceID    string          `json:"instanceId"`
	InstanceName  string          `json:"instanceName"`
	InstanceToken string          `json:"instanceToken"`
	Data          json.RawMessage `json:"data"`
}

// ChatwootConfig espelha o painel da integração antiga (ChatwootDto em
// evolutionapi_antiga/src/api/integrations/chatbot/chatwoot/dto/chatwoot.dto.ts).
type ChatwootConfig struct {
	InstanceID              string   `json:"instanceId"`
	InstanceName            string   `json:"instanceName"`
	Enabled                 bool     `json:"enabled"`
	URL                     string   `json:"url"`
	AccountID               string   `json:"accountId"`
	Token                   string   `json:"token"`
	NameInbox               string   `json:"nameInbox"`
	SignMsg                 bool     `json:"signMsg"`
	SignDelimiter           string   `json:"signDelimiter"`
	Number                  string   `json:"number"`
	ReopenConversation      bool     `json:"reopenConversation"`
	ConversationPending     bool     `json:"conversationPending"`
	MergeBrazilContacts     bool     `json:"mergeBrazilContacts"`
	ImportContacts          bool     `json:"importContacts"`
	ImportMessages          bool     `json:"importMessages"`
	DaysLimitImportMessages int      `json:"daysLimitImportMessages"`
	AutoCreate              bool     `json:"autoCreate"`
	Organization            string   `json:"organization"`
	Logo                    string   `json:"logo"`
	IgnoreJids              []string `json:"ignoreJids"`
	// InboxID é preenchido pelo provisionamento (não vem do formulário).
	InboxID int `json:"inboxId,omitempty"`
}

// ContactMapping liga um JID do WhatsApp a um contato do Chatwoot.
type ContactMapping struct {
	InstanceID        string
	RemoteJid         string
	ChatwootContactID int
	Identifier        string
	UpdatedAt         time.Time
}

// ConversationMapping liga um chat do WhatsApp a uma conversa do Chatwoot.
type ConversationMapping struct {
	InstanceID             string
	RemoteJid              string
	ChatwootConversationID int
	InboxID                int
	Status                 string // open | pending | resolved
	UpdatedAt              time.Time
}

// MessageMapping deduplica e correlaciona mensagens nos dois sentidos.
type MessageMapping struct {
	InstanceID        string
	WhatsappMessageID string // key.id do WhatsApp
	ChatwootMessageID int
	Direction         string // in (wa->cw) | out (cw->wa)
	CreatedAt         time.Time
}
