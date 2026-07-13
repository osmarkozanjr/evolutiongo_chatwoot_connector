// client_impl.go — implementação HTTP do chatwoot.Client (Agente B).
//
// Todos os paths/campos rastreiam para:
//   - chatwoot/config/routes.rb (api/v1/accounts/...)
//   - evolutionapi_antiga/.../chatwoot/services/chatwoot.service.ts (uso real do SDK)
//
// Autenticação: header `api_access_token`.
// Base: {cfg.URL}/api/v1/accounts/{cfg.AccountID}
package chatwoot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
)

// HTTPClient é a implementação padrão de Client sobre net/http.
type HTTPClient struct {
	http *http.Client
}

// attachmentQuoteEscaper escapa filename para o Content-Disposition da parte
// multipart (mesma regra do escapeQuotes não exportado de mime/multipart).
var attachmentQuoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

// NewHTTPClient cria um Client com timeouts sensatos.
func NewHTTPClient() *HTTPClient {
	return &HTTPClient{
		http: &http.Client{Timeout: 30 * time.Second},
	}
}

// NewHTTPClientWith permite injetar um *http.Client (útil em testes).
func NewHTTPClientWith(h *http.Client) *HTTPClient {
	if h == nil {
		h = &http.Client{Timeout: 30 * time.Second}
	}
	return &HTTPClient{http: h}
}

var _ Client = (*HTTPClient)(nil)

// baseURL monta {url}/api/v1/accounts/{accountId}.
func baseURL(cfg *model.ChatwootConfig) string {
	return strings.TrimRight(cfg.URL, "/") + "/api/v1/accounts/" + cfg.AccountID
}

// doJSON executa uma requisição JSON com retry leve em 5xx e decodifica em out.
// out pode ser nil quando o corpo não interessa.
func (c *HTTPClient) doJSON(ctx context.Context, cfg *model.ChatwootConfig, method, path string, body any, out any) error {
	var payload []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("chatwoot: marshal body: %w", err)
		}
		payload = b
	}

	url := baseURL(cfg) + path
	respBody, err := c.do(ctx, cfg, method, url, "application/json", func() io.Reader {
		if payload == nil {
			return nil
		}
		return bytes.NewReader(payload)
	})
	if err != nil {
		return err
	}
	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("chatwoot: decode response (%s %s): %w", method, path, err)
		}
	}
	return nil
}

// APIError representa uma resposta de erro (>=400) do Chatwoot, preservando o
// status e o corpo bruto para que os chamadores possam reagir a casos
// específicos (ex.: 422 "Identifier has already been taken").
type APIError struct {
	StatusCode int
	Method     string
	URL        string
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("chatwoot: %s %s returned %d: %s", e.Method, e.URL, e.StatusCode, e.Body)
}

// isIdentifierTaken indica um 422 cujo corpo acusa colisão do campo identifier
// (contato já existe no Chatwoot com o mesmo JID).
func (e *APIError) isIdentifierTaken() bool {
	return e.StatusCode == http.StatusUnprocessableEntity &&
		strings.Contains(e.Body, "Identifier has already been taken")
}

// do executa a requisição HTTP com retry leve em 5xx e devolve o corpo bruto.
// bodyFn é chamado a cada tentativa para reabrir o reader.
func (c *HTTPClient) do(ctx context.Context, cfg *model.ChatwootConfig, method, url, contentType string, bodyFn func() io.Reader) ([]byte, error) {
	const maxAttempts = 3
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bodyFn())
		if err != nil {
			return nil, fmt.Errorf("chatwoot: build request: %w", err)
		}
		req.Header.Set("api_access_token", cfg.Token)
		req.Header.Set("Accept", "application/json")
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("chatwoot: request %s %s: %w", method, url, err)
			if attempt < maxAttempts {
				time.Sleep(time.Duration(attempt) * 300 * time.Millisecond)
				continue
			}
			return nil, lastErr
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("chatwoot: %s %s returned %d: %s", method, url, resp.StatusCode, string(respBody))
			if attempt < maxAttempts {
				time.Sleep(time.Duration(attempt) * 300 * time.Millisecond)
				continue
			}
			return nil, lastErr
		}
		if resp.StatusCode >= 400 {
			return nil, &APIError{StatusCode: resp.StatusCode, Method: method, URL: url, Body: string(respBody)}
		}
		return respBody, nil
	}
	return nil, lastErr
}

// --- Inboxes -----------------------------------------------------------------

type inboxDTO struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type inboxListDTO struct {
	Payload []inboxDTO `json:"payload"`
}

// EnsureInbox provisiona um inbox de canal API (porta de initInstanceChatwoot).
// GET inboxes → se já existe pelo nome, reutiliza; senão POST inboxes com
// channel {type:"api", webhook_url}.
func (c *HTTPClient) EnsureInbox(ctx context.Context, cfg *model.ChatwootConfig, webhookURL string) (int, error) {
	name := cfg.NameInbox
	if name == "" {
		name = cfg.InstanceName
	}

	// Evita duplicata pelo nome (checkDuplicate em initInstanceChatwoot).
	var list inboxListDTO
	if err := c.doJSON(ctx, cfg, http.MethodGet, "/inboxes", nil, &list); err != nil {
		return 0, err
	}
	for _, ib := range list.Payload {
		if ib.Name == name {
			return ib.ID, nil
		}
	}

	// Cria inbox de canal API. Shape verificado em initInstanceChatwoot():
	// data: { name, channel: { type: 'api', webhook_url } }
	reqBody := map[string]any{
		"name": name,
		"channel": map[string]any{
			"type":        "api",
			"webhook_url": webhookURL,
		},
	}
	var created inboxDTO
	if err := c.doJSON(ctx, cfg, http.MethodPost, "/inboxes", reqBody, &created); err != nil {
		return 0, err
	}
	if created.ID == 0 {
		return 0, fmt.Errorf("chatwoot: inbox criado sem id")
	}
	return created.ID, nil
}

// UpdateInboxWebhook atualiza o webhook_url do canal API do inbox.
// PATCH inboxes/{id} com channel.webhook_url (routes.rb: inboxes update).
// TODO(VERIFY): a integração antiga não atualiza o webhook após criar; o shape
// exato do update de channel não foi observado em uso — confirmar contra
// chatwoot/app/controllers/api/v1/accounts/inboxes_controller.rb se necessário.
func (c *HTTPClient) UpdateInboxWebhook(ctx context.Context, cfg *model.ChatwootConfig, inboxID int, webhookURL string) error {
	reqBody := map[string]any{
		"channel": map[string]any{
			"webhook_url": webhookURL,
		},
	}
	return c.doJSON(ctx, cfg, http.MethodPatch, "/inboxes/"+strconv.Itoa(inboxID), reqBody, nil)
}

// --- Contatos ----------------------------------------------------------------

type contactSearchDTO struct {
	Payload []Contact `json:"payload"`
}

// SearchContact busca via GET contacts/search?q= (porta de findContactByIdentifier).
// Retorna nil,nil quando nada é encontrado.
func (c *HTTPClient) SearchContact(ctx context.Context, cfg *model.ChatwootConfig, phoneOrIdentifier string) (*Contact, error) {
	q := url.QueryEscape(phoneOrIdentifier)
	var res contactSearchDTO
	if err := c.doJSON(ctx, cfg, http.MethodGet, "/contacts/search?q="+q+"&sort=name", nil, &res); err != nil {
		return nil, err
	}
	if len(res.Payload) == 0 {
		return nil, nil
	}
	ct := res.Payload[0]
	return &ct, nil
}

// contactCreateDTO cobre os dois formatos observados em createContact():
// resposta pode vir como { id } direto ou { payload: { contact: {...} } }.
type contactCreateDTO struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Identifier  string `json:"identifier"`
	PhoneNumber string `json:"phone_number"`
	AvatarURL   string `json:"avatar_url"`
	Payload     *struct {
		Contact Contact `json:"contact"`
	} `json:"payload"`
}

// CreateContact cria um contato (POST contacts). Shape verificado em createContact():
// { inbox_id, name, identifier, avatar_url, phone_number:"+{phone}" }.
func (c *HTTPClient) CreateContact(ctx context.Context, cfg *model.ChatwootConfig, inboxID int, phone, name, avatarURL, identifier string) (*Contact, error) {
	if name == "" {
		name = phone
	}
	data := map[string]any{
		"inbox_id":   inboxID,
		"name":       name,
		"identifier": identifier,
		"avatar_url": avatarURL,
	}
	// createContact só define phone_number quando o jid contém '@' ou está vazio.
	// Normaliza para um único '+' inicial: os chamadores já passam o número em
	// E.164 ("+55…"), então prefixar cegamente geraria "++55…" — telefone
	// inválido que quebra a busca por telefone dos próximos eventos.
	if identifier == "" || strings.Contains(identifier, "@") {
		if phone != "" {
			data["phone_number"] = "+" + strings.TrimPrefix(phone, "+")
		}
	}

	var res contactCreateDTO
	if err := c.doJSON(ctx, cfg, http.MethodPost, "/contacts", data, &res); err != nil {
		// Idempotência: se o contato já existe no Chatwoot com esse identifier
		// (JID), o POST devolve 422 "Identifier has already been taken". Nesse
		// caso resolvemos o contato existente pelo identifier em vez de falhar
		// — necessário após perder o mapping local (ex.: recriação do banco),
		// quando a busca por telefone não encontra o contato (variantes BR do
		// 9º dígito, etc.).
		var apiErr *APIError
		if identifier != "" && errors.As(err, &apiErr) && apiErr.isIdentifierTaken() {
			existing, sErr := c.SearchContact(ctx, cfg, identifier)
			if sErr != nil {
				return nil, fmt.Errorf("chatwoot: identifier %q já existe mas falhou ao buscá-lo: %w", identifier, sErr)
			}
			if existing != nil {
				return existing, nil
			}
		}
		return nil, err
	}
	if res.Payload != nil && res.Payload.Contact.ID != 0 {
		ct := res.Payload.Contact
		return &ct, nil
	}
	return &Contact{
		ID:          res.ID,
		Name:        res.Name,
		Identifier:  res.Identifier,
		PhoneNumber: res.PhoneNumber,
		AvatarURL:   res.AvatarURL,
	}, nil
}

// UpdateContact atualiza campos do contato (PUT contacts/{id}).
func (c *HTTPClient) UpdateContact(ctx context.Context, cfg *model.ChatwootConfig, contactID int, fields map[string]any) error {
	return c.doJSON(ctx, cfg, http.MethodPut, "/contacts/"+strconv.Itoa(contactID), fields, nil)
}

// MergeContacts funde dois contatos.
// POST actions/contact_merge com { base_contact_id, mergee_contact_id }.
// Confirmado em routes.rb (namespace :actions → resource :contact_merge) e em
// mergeContacts() na integração antiga (url actions/contact_merge).
func (c *HTTPClient) MergeContacts(ctx context.Context, cfg *model.ChatwootConfig, baseID, mergeID int) error {
	data := map[string]any{
		"base_contact_id":   baseID,
		"mergee_contact_id": mergeID,
	}
	return c.doJSON(ctx, cfg, http.MethodPost, "/actions/contact_merge", data, nil)
}

// --- Conversas ---------------------------------------------------------------

type conversationListDTO struct {
	Payload []Conversation `json:"payload"`
}

// GetOpenConversation acha a conversa 'open' do contato no inbox
// (porta de getOpenConversationByContact: GET contacts/{id}/conversations).
func (c *HTTPClient) GetOpenConversation(ctx context.Context, cfg *model.ChatwootConfig, contactID, inboxID int) (*Conversation, error) {
	var res conversationListDTO
	path := "/contacts/" + strconv.Itoa(contactID) + "/conversations"
	if err := c.doJSON(ctx, cfg, http.MethodGet, path, nil, &res); err != nil {
		return nil, err
	}
	for _, cv := range res.Payload {
		if cv.InboxID == inboxID && cv.Status == "open" {
			conv := cv
			return &conv, nil
		}
	}
	return nil, nil
}

// CreateConversation cria uma conversa (POST conversations).
// Shape verificado em createConversation(): { contact_id, inbox_id, status? }.
func (c *HTTPClient) CreateConversation(ctx context.Context, cfg *model.ChatwootConfig, contactID, inboxID int, status string) (*Conversation, error) {
	data := map[string]any{
		"contact_id": strconv.Itoa(contactID),
		"inbox_id":   strconv.Itoa(inboxID),
	}
	if status != "" {
		data["status"] = status
	}
	var conv Conversation
	if err := c.doJSON(ctx, cfg, http.MethodPost, "/conversations", data, &conv); err != nil {
		return nil, err
	}
	return &conv, nil
}

// ToggleConversationStatus altera o status (POST conversations/{id}/toggle_status).
// Confirmado em routes.rb (member post :toggle_status).
func (c *HTTPClient) ToggleConversationStatus(ctx context.Context, cfg *model.ChatwootConfig, conversationID int, status string) error {
	data := map[string]any{"status": status}
	path := "/conversations/" + strconv.Itoa(conversationID) + "/toggle_status"
	return c.doJSON(ctx, cfg, http.MethodPost, path, data, nil)
}

// --- Mensagens ---------------------------------------------------------------

// messageRespDTO só extrai o que o conector usa da resposta de criação.
// message_type na API do Chatwoot pode vir como número (0/1) ou string, então
// não o mapeamos aqui — preservamos o messageType enviado pelo chamador.
type messageRespDTO struct {
	ID             int    `json:"id"`
	Content        string `json:"content"`
	ConversationID int    `json:"conversation_id"`
}

// CreateMessage cria uma mensagem na conversa.
// POST conversations/{conversationId}/messages.
// Quando há attachments, usa multipart/form-data com attachments[] (porta de
// sendData()); caso contrário, JSON simples.
func (c *HTTPClient) CreateMessage(ctx context.Context, cfg *model.ChatwootConfig, conversationID int, content, messageType string, attachments []Attachment, sourceID string) (*Message, error) {
	path := "/conversations/" + strconv.Itoa(conversationID) + "/messages"

	var resp messageRespDTO

	if len(attachments) == 0 {
		data := map[string]any{
			"message_type": messageType,
		}
		if content != "" {
			data["content"] = content
		}
		if sourceID != "" {
			data["source_id"] = sourceID
		}
		if err := c.doJSON(ctx, cfg, http.MethodPost, path, data, &resp); err != nil {
			return nil, err
		}
		return &Message{ID: resp.ID, Content: resp.Content, MessageType: messageType, ConversationID: conversationID}, nil
	}

	// Multipart: campos + attachments[] (verificado em sendData()).
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	if content != "" {
		_ = w.WriteField("content", content)
	}
	_ = w.WriteField("message_type", messageType)
	if sourceID != "" {
		_ = w.WriteField("source_id", sourceID)
	}
	for _, att := range attachments {
		// CreateFormFile fixaria Content-Type: application/octet-stream na
		// parte, e o Chatwoot usa esse content-type para classificar o anexo
		// (imagem inline vs. arquivo para download) — então a parte é criada
		// manualmente com o mime real do anexo.
		hdr := textproto.MIMEHeader{}
		hdr.Set("Content-Disposition", `form-data; name="attachments[]"; filename="`+attachmentQuoteEscaper.Replace(att.Filename)+`"`)
		mime := att.Mime
		if mime == "" {
			mime = "application/octet-stream"
		}
		hdr.Set("Content-Type", mime)
		part, err := w.CreatePart(hdr)
		if err != nil {
			return nil, fmt.Errorf("chatwoot: multipart part: %w", err)
		}
		if _, err := part.Write(att.Data); err != nil {
			return nil, fmt.Errorf("chatwoot: multipart write: %w", err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("chatwoot: multipart close: %w", err)
	}

	raw := buf.Bytes()
	respBody, err := c.do(ctx, cfg, http.MethodPost, baseURL(cfg)+path, w.FormDataContentType(), func() io.Reader {
		return bytes.NewReader(raw)
	})
	if err != nil {
		return nil, err
	}
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &resp); err != nil {
			return nil, fmt.Errorf("chatwoot: decode message response: %w", err)
		}
	}
	return &Message{ID: resp.ID, Content: resp.Content, MessageType: messageType, ConversationID: conversationID}, nil
}

// DeleteMessage remove uma mensagem (DELETE conversations/{id}/messages/{id}).
func (c *HTTPClient) DeleteMessage(ctx context.Context, cfg *model.ChatwootConfig, conversationID, messageID int) error {
	path := "/conversations/" + strconv.Itoa(conversationID) + "/messages/" + strconv.Itoa(messageID)
	return c.doJSON(ctx, cfg, http.MethodDelete, path, nil, nil)
}
