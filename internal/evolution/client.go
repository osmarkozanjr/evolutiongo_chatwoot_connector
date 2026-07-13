// Package evolution define o client da API HTTP do evolutiongo.
// A implementação vive em client_impl.go (Agente B).
// Contratos verificados em evolutiongo_original/pkg/routes/routes.go e
// evolutiongo_original/pkg/sendMessage/service/send_service.go.
// Autenticação: header "apikey" com o token da instância
// (evolutiongo_original/pkg/middleware/auth_middleware.go).
package evolution

import "context"

// Quoted espelha QuotedStruct do evolutiongo.
type Quoted struct {
	MessageID   string `json:"messageId,omitempty"`
	Participant string `json:"participant,omitempty"`
}

// TextMessage espelha TextStruct (POST /send/text).
type TextMessage struct {
	Number string  `json:"number"`
	Text   string  `json:"text"`
	Quoted *Quoted `json:"quoted,omitempty"`
}

// MediaMessage espelha MediaStruct (POST /send/media).
type MediaMessage struct {
	Number   string  `json:"number"`
	URL      string  `json:"url"`
	Type     string  `json:"type"` // image | video | audio | document
	Caption  string  `json:"caption,omitempty"`
	Filename string  `json:"filename,omitempty"`
	Quoted   *Quoted `json:"quoted,omitempty"`
}

// SendResult carrega o id da mensagem enviada (para dedup/correlação).
type SendResult struct {
	MessageID string
	Raw       map[string]any
}

// Client fala com o evolutiongo (base URL interna do service, ex. http://evolution_go:4000),
// autenticando com o token da instância no header "apikey".
type Client interface {
	SendText(ctx context.Context, instanceToken string, msg *TextMessage) (*SendResult, error)
	SendMedia(ctx context.Context, instanceToken string, msg *MediaMessage) (*SendResult, error)
	// FetchProfilePicture busca o avatar de um número (POST /user/avatar), se disponível.
	FetchProfilePicture(ctx context.Context, instanceToken, number string) (url string, err error)
	// CheckNumber valida/normaliza um número no WhatsApp (POST /user/check).
	CheckNumber(ctx context.Context, instanceToken, number string) (jid string, exists bool, err error)
}
