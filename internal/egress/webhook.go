// Package egress trata o sentido Chatwoot → WhatsApp: recebe o webhook do
// inbox API do Chatwoot e envia as mensagens via evolutiongo.
//
// Porta de receiveWebhook() em
// evolutionapi_antiga/src/api/integrations/chatbot/chatwoot/services/chatwoot.service.ts.
package egress

import (
	"mime"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
)

// --- Payload do webhook --------------------------------------------------------
// Campos rastreados ao uso real em receiveWebhook(): body.event, body.id,
// body.content, body.message_type, body.private, body.status,
// body.content_attributes.deleted, body.inbox, body.sender.name, body.meta.sender,
// body.conversation.{id, meta.sender, messages[], contact_inbox.source_id}.

// WebhookPayload é o corpo POSTado pelo Chatwoot no webhook do inbox API.
type WebhookPayload struct {
	Event             string             `json:"event"`
	ID                int                `json:"id"`
	Content           string             `json:"content"`
	MessageType       string             `json:"message_type"`
	Private           bool               `json:"private"`
	SourceID          string             `json:"source_id"`
	Status            string             `json:"status"`
	ContentAttributes *ContentAttributes `json:"content_attributes"`
	Inbox             *InboxRef          `json:"inbox"`
	Sender            *SenderRef         `json:"sender"`
	Meta              *Meta              `json:"meta"`
	Conversation      *ConversationRef   `json:"conversation"`
}

// ContentAttributes carrega os atributos usados pelo conector.
type ContentAttributes struct {
	Deleted   bool `json:"deleted"`
	InReplyTo int  `json:"in_reply_to"`
}

// InboxRef identifica o inbox no payload.
type InboxRef struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// SenderRef identifica o remetente (agente) no payload.
type SenderRef struct {
	Name          string `json:"name"`
	AvailableName string `json:"available_name"`
}

// Meta embrulha o sender da conversa.
type Meta struct {
	Sender *MetaSender `json:"sender"`
}

// MetaSender é o contato da conversa (identifier = JID do WhatsApp).
type MetaSender struct {
	Identifier  string `json:"identifier"`
	PhoneNumber string `json:"phone_number"`
}

// ConversationRef é o subconjunto de body.conversation usado.
type ConversationRef struct {
	ID           int           `json:"id"`
	Meta         *Meta         `json:"meta"`
	Messages     []ConvMessage `json:"messages"`
	ContactInbox *ContactInbox `json:"contact_inbox"`
}

// ContactInbox carrega o source_id do contact_inbox.
type ContactInbox struct {
	SourceID string `json:"source_id"`
}

// ConvMessage é uma mensagem embutida em body.conversation.messages.
type ConvMessage struct {
	ID          int             `json:"id"`
	Content     string          `json:"content"`
	SourceID    string          `json:"source_id"`
	Sender      *SenderRef      `json:"sender"`
	Attachments []AttachmentRef `json:"attachments"`
}

// AttachmentRef é um anexo do Chatwoot (data_url aponta para o arquivo).
type AttachmentRef struct {
	ID       int    `json:"id"`
	DataURL  string `json:"data_url"`
	FileType string `json:"file_type"`
}

// chatID resolve o destino WhatsApp da conversa:
// identifier ou phone_number sem '+' (porta exata do receiveWebhook).
func (p *WebhookPayload) chatID() string {
	if p.Conversation == nil || p.Conversation.Meta == nil || p.Conversation.Meta.Sender == nil {
		return ""
	}
	s := p.Conversation.Meta.Sender
	if s.Identifier != "" {
		return s.Identifier
	}
	return strings.TrimPrefix(s.PhoneNumber, "+")
}

// metaSenderIdentifier lê body.meta.sender.identifier (usado no evento
// conversation_status_changed, onde não há body.conversation).
func (p *WebhookPayload) metaSenderIdentifier() string {
	if p.Meta == nil || p.Meta.Sender == nil {
		return ""
	}
	return p.Meta.Sender.Identifier
}

// senderName resolve a assinatura do agente:
// conversation.messages[0].sender.available_name || body.sender.name.
func (p *WebhookPayload) senderName() string {
	if p.Conversation != nil && len(p.Conversation.Messages) > 0 {
		if s := p.Conversation.Messages[0].Sender; s != nil && s.AvailableName != "" {
			return s.AvailableName
		}
	}
	if p.Sender != nil {
		return p.Sender.Name
	}
	return ""
}

// --- Handler HTTP ---------------------------------------------------------------

// Handler devolve o gin.HandlerFunc para POST /chatwoot/webhook/:instanceId.
// Como o serviço antigo, sempre responde {"message":"bot"} em caso de sucesso
// (o Chatwoot não deve reentregar por erro de negócio).
func Handler(svc *Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		instanceID := c.Param("instanceId")
		if instanceID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "instanceId is required"})
			return
		}

		var payload WebhookPayload
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if err := svc.HandleWebhook(c.Request.Context(), instanceID, &payload); err != nil {
			// Erros de envio já geram mensagem de erro na conversa
			// (onSendMessageError); o antigo também engolia o erro e devolvia bot.
			c.JSON(http.StatusOK, gin.H{"message": "bot", "error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "bot"})
	}
}

// --- Conversão de texto Chatwoot → WhatsApp --------------------------------------
// Porta das substituições de receiveWebhook():
//   *x*   -> _x_
//   **x** -> *x*
//   ~~x~~ -> ~x~
//   `x`   -> ```x```
// Os regexes originais usam lookaround (indisponível no RE2 do Go); a mesma
// semântica é obtida protegendo os pares duplos com placeholders antes de
// converter os simples.

const (
	phDoubleStar = "\x00" // protege ** durante a conversão de *
	phTripleTick = "\x01" // protege ``` durante a conversão de `
)

var (
	reDoubleStar  = regexp.MustCompile(`\*\*([^\s*][^\n*]*?[^\s*]|[^\s*])\*\*`)
	reSingleStar  = regexp.MustCompile(`\*([^\s*][^\n*]*?[^\s*]|[^\s*])\*`)
	reDoubleTilde = regexp.MustCompile(`~~([^\s~][^\n~]*?[^\s~]|[^\s~])~~`)
	reSingleTick  = regexp.MustCompile("`([^\\s`*][^`*\n]*?[^\\s`*]|[^\\s`*])`")
)

// convertMarkdown converte o markdown do Chatwoot para o formato do WhatsApp.
func convertMarkdown(content string) string {
	if content == "" {
		return content
	}
	out := content
	// Protege pares que não devem ser reinterpretados pelos regexes simples.
	out = reDoubleStar.ReplaceAllString(out, phDoubleStar+"$1"+phDoubleStar)
	out = strings.ReplaceAll(out, "```", phTripleTick)

	// ${1} entre chaves: "$1_" seria lido como grupo "1_" (gotcha do Go).
	out = reSingleStar.ReplaceAllString(out, "_${1}_")
	out = reDoubleTilde.ReplaceAllString(out, "~$1~")
	out = reSingleTick.ReplaceAllString(out, "```$1```")

	out = strings.ReplaceAll(out, phDoubleStar, "*")
	out = strings.ReplaceAll(out, phTripleTick, "```")
	return out
}

// formatOutgoingText monta o texto final com assinatura do agente
// (porta do bloco signMsg/signDelimiter de receiveWebhook()).
func formatOutgoingText(content, senderName string, signMsg bool, signDelimiter string) string {
	msg := convertMarkdown(content)
	if senderName == "" {
		return msg
	}
	delim := "\n"
	if signDelimiter != "" {
		delim = strings.ReplaceAll(signDelimiter, "\\n", "\n")
	}
	var parts []string
	if signMsg {
		parts = append(parts, "*"+senderName+":*")
	}
	parts = append(parts, msg)
	return strings.Join(parts, delim)
}

// --- Dedução de tipo de mídia -----------------------------------------------------
// Porta de sendAttachment(): tipo derivado do mime da extensão do data_url;
// extensões "de documento" forçam type=document mesmo sendo imagem.

// documentExtensions replica a lista de sendAttachment().
var documentExtensions = map[string]bool{
	".gif": true, ".svg": true, ".tiff": true, ".tif": true, ".dxf": true, ".dwg": true,
}

// extMime é uma tabela determinística das extensões comuns (o antigo usava a
// base do pacote mime-types do Node; aqui evitamos depender do /etc/mime.types
// do host para manter o comportamento estável).
var extMime = map[string]string{
	".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
	".webp": "image/webp", ".gif": "image/gif", ".bmp": "image/bmp",
	".svg": "image/svg+xml", ".tiff": "image/tiff", ".tif": "image/tiff",
	".mp4": "video/mp4", ".3gp": "video/3gpp", ".mov": "video/quicktime",
	".avi": "video/x-msvideo", ".mkv": "video/x-matroska",
	".mp3": "audio/mpeg", ".ogg": "audio/ogg", ".oga": "audio/ogg",
	".opus": "audio/opus", ".wav": "audio/wav", ".m4a": "audio/mp4",
	".aac": "audio/aac", ".amr": "audio/amr",
	".pdf": "application/pdf",
}

// deduceAttachment resolve (type, filename) para o /send/media do evolutiongo
// a partir do data_url do anexo do Chatwoot.
func deduceAttachment(dataURL string) (mediaType, filename string) {
	decoded, err := url.PathUnescape(dataURL)
	if err != nil {
		decoded = dataURL
	}
	// Remove query/fragment antes de extrair o nome (path.parse no antigo).
	if i := strings.IndexAny(decoded, "?#"); i >= 0 {
		decoded = decoded[:i]
	}
	filename = path.Base(decoded)
	ext := strings.ToLower(path.Ext(filename))

	mimeType := extMime[ext]
	if mimeType == "" {
		mimeType = mime.TypeByExtension(ext)
	}
	// TODO(VERIFY): o antigo, sem mime pela extensão, baixava o arquivo e lia o
	// Content-Type. Aqui caímos em "document" para não fazer rede no parse;
	// o evolutiongo já valida o mime real ao baixar a URL.

	mediaType = "document"
	switch {
	case strings.HasPrefix(mimeType, "image/"):
		mediaType = "image"
	case strings.HasPrefix(mimeType, "video/"):
		mediaType = "video"
	case strings.HasPrefix(mimeType, "audio/"):
		mediaType = "audio"
	}
	if mediaType == "image" && documentExtensions[ext] {
		mediaType = "document"
	}
	return mediaType, filename
}

// normalizeTemplateText replica body.content.replace(/\\\r\n|\\\n|\n/g, '\n')
// usado para message_type=template.
var reTemplateBreaks = regexp.MustCompile(`\\\r\n|\\\n|\n`)

func normalizeTemplateText(content string) string {
	return reTemplateBreaks.ReplaceAllString(content, "\n")
}
