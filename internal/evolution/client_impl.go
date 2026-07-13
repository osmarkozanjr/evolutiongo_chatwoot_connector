// client_impl.go — implementação HTTP do evolution.Client (Agente B).
//
// Contratos verificados em:
//   - evolutiongo_original/pkg/routes/routes.go (rotas /send/*, /user/*)
//   - evolutiongo_original/pkg/sendMessage/service/send_service.go (structs de request)
//   - evolutiongo_original/pkg/sendMessage/handler/send_handler.go (shape da resposta)
//   - evolutiongo_original/pkg/user/handler + service (avatar/check)
//   - evolutiongo_original/pkg/middleware/auth_middleware.go (header apikey)
//
// Autenticação: header `apikey` = token da instância.
package evolution

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTPClient implementa Client sobre net/http.
// baseURL é a URL interna do evolutiongo (ex. http://evolution_go:4000).
type HTTPClient struct {
	baseURL string
	http    *http.Client
}

// NewHTTPClient cria um Client apontando para o evolutiongo.
func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: 60 * time.Second},
	}
}

// NewHTTPClientWith permite injetar um *http.Client (útil em testes).
func NewHTTPClientWith(baseURL string, h *http.Client) *HTTPClient {
	if h == nil {
		h = &http.Client{Timeout: 60 * time.Second}
	}
	return &HTTPClient{baseURL: strings.TrimRight(baseURL, "/"), http: h}
}

var _ Client = (*HTTPClient)(nil)

// postJSON executa POST com header apikey e retry leve em 5xx; devolve o corpo bruto.
func (c *HTTPClient) postJSON(ctx context.Context, instanceToken, path string, body any) ([]byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("evolution: marshal body: %w", err)
	}

	const maxAttempts = 3
	var lastErr error
	url := c.baseURL + path

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return nil, fmt.Errorf("evolution: build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		req.Header.Set("apikey", instanceToken)

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("evolution: request POST %s: %w", path, err)
			if attempt < maxAttempts {
				time.Sleep(time.Duration(attempt) * 300 * time.Millisecond)
				continue
			}
			return nil, lastErr
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("evolution: POST %s returned %d: %s", path, resp.StatusCode, string(respBody))
			if attempt < maxAttempts {
				time.Sleep(time.Duration(attempt) * 300 * time.Millisecond)
				continue
			}
			return nil, lastErr
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("evolution: POST %s returned %d: %s", path, resp.StatusCode, string(respBody))
		}
		return respBody, nil
	}
	return nil, lastErr
}

// sendEnvelope espelha a resposta dos handlers de /send/*:
//   ctx.JSON(200, gin.H{"message":"success","data": *MessageSendStruct})
// onde MessageSendStruct.Info é types.MessageInfo (whatsmeow), cujo campo ID
// (sem json tag) serializa como "ID". Portanto o id da mensagem WA está em
// data.Info.ID (verificado em send_service.go: MessageSendStruct + send_handler.go).
type sendEnvelope struct {
	Message string `json:"message"`
	Data    struct {
		Info struct {
			ID string `json:"ID"`
		} `json:"Info"`
	} `json:"data"`
}

// parseSendResult extrai o message id de forma tolerante.
func parseSendResult(body []byte) (*SendResult, error) {
	var env sendEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("evolution: decode send response: %w", err)
	}

	raw := map[string]any{}
	_ = json.Unmarshal(body, &raw)

	id := env.Data.Info.ID
	if id == "" {
		// Fallback tolerante: procura data.Info.ID / data.key.id em profundidade.
		// TODO(VERIFY): confirmar que o handler sempre retorna Info.ID não-vazio;
		// o shape base foi verificado, mas mantemos fallback por robustez.
		id = digInfoID(raw)
	}

	return &SendResult{MessageID: id, Raw: raw}, nil
}

// digInfoID tenta achar o id da mensagem em formatos alternativos.
func digInfoID(raw map[string]any) string {
	data, _ := raw["data"].(map[string]any)
	if data == nil {
		return ""
	}
	if info, ok := data["Info"].(map[string]any); ok {
		if id, ok := info["ID"].(string); ok {
			return id
		}
	}
	if key, ok := data["key"].(map[string]any); ok {
		if id, ok := key["id"].(string); ok {
			return id
		}
	}
	return ""
}

// SendText envia texto (POST /send/text).
func (c *HTTPClient) SendText(ctx context.Context, instanceToken string, msg *TextMessage) (*SendResult, error) {
	body, err := c.postJSON(ctx, instanceToken, "/send/text", msg)
	if err != nil {
		return nil, err
	}
	return parseSendResult(body)
}

// SendMedia envia mídia via URL (POST /send/media, corpo JSON).
func (c *HTTPClient) SendMedia(ctx context.Context, instanceToken string, msg *MediaMessage) (*SendResult, error) {
	body, err := c.postJSON(ctx, instanceToken, "/send/media", msg)
	if err != nil {
		return nil, err
	}
	return parseSendResult(body)
}

// avatarEnvelope espelha a resposta de /user/avatar:
//   gin.H{"message":"success","data": *types.ProfilePictureInfo}
// ProfilePictureInfo tem json tag "url" (verificado no user_service.go).
type avatarEnvelope struct {
	Data struct {
		URL string `json:"url"`
	} `json:"data"`
}

// FetchProfilePicture busca o avatar de um número (POST /user/avatar).
func (c *HTTPClient) FetchProfilePicture(ctx context.Context, instanceToken, number string) (string, error) {
	// GetAvatarStruct: { number, preview } (user_service.go).
	reqBody := map[string]any{"number": number}
	body, err := c.postJSON(ctx, instanceToken, "/user/avatar", reqBody)
	if err != nil {
		return "", err
	}
	var env avatarEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return "", fmt.Errorf("evolution: decode avatar response: %w", err)
	}
	return env.Data.URL, nil
}

// checkEnvelope espelha a resposta de /user/check:
//   gin.H{"message":"success","data": *CheckUserCollection}
// CheckUserCollection.Users é []User{Query,IsInWhatsapp,JID,RemoteJID,...}
// (verificado em user_service.go).
type checkEnvelope struct {
	Data struct {
		Users []struct {
			Query        string `json:"Query"`
			IsInWhatsapp bool   `json:"IsInWhatsapp"`
			JID          string `json:"JID"`
			RemoteJID    string `json:"RemoteJID"`
		} `json:"Users"`
	} `json:"data"`
}

// CheckNumber valida/normaliza um número no WhatsApp (POST /user/check).
// CheckUserStruct.Number é um array de strings (user_service.go), por isso
// enviamos {"number":[number]}.
func (c *HTTPClient) CheckNumber(ctx context.Context, instanceToken, number string) (string, bool, error) {
	reqBody := map[string]any{"number": []string{number}}
	body, err := c.postJSON(ctx, instanceToken, "/user/check", reqBody)
	if err != nil {
		return "", false, err
	}
	var env checkEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return "", false, fmt.Errorf("evolution: decode check response: %w", err)
	}
	if len(env.Data.Users) == 0 {
		return "", false, nil
	}
	u := env.Data.Users[0]
	jid := u.RemoteJID
	if jid == "" {
		jid = u.JID
	}
	return jid, u.IsInWhatsapp, nil
}
