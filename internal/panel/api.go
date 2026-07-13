// Package panel implementa a API e a UI de configuração da integração
// Chatwoot por instância — equivalente ao painel da integração antiga
// (evolutionapi_antiga/src/api/integrations/chatbot/chatwoot).
//
// Semântica de validação replicada de
// evolutionapi_antiga/.../controllers/chatwoot.controller.ts:
//   - enabled=true exige url (válida), accountId e token.
//   - signMsg é obrigatório quando enabled=true; se signMsg=false, o
//     signDelimiter é zerado.
//   - nameInbox, quando vazio, cai para o nome/identificador da instância.
//   - find() sempre devolve webhook_url calculada a partir da URL pública
//     do conector: {PublicURL}/chatwoot/webhook/{instanceId}.
package panel

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/iceasa/evolution-chatwoot-connector/internal/chatwoot"
	"github.com/iceasa/evolution-chatwoot-connector/internal/config"
	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
	"github.com/iceasa/evolution-chatwoot-connector/internal/store"
)

// Handler concentra as dependências da API do painel. Construído pelo
// wire-up (app.go, de responsabilidade do orquestrador) e registrado num
// *gin.Engine via Register.
type Handler struct {
	Store    store.Store
	Chatwoot chatwoot.Client
	Config   *config.Config
}

// NewHandler cria o handler do painel.
func NewHandler(st store.Store, cw chatwoot.Client, cfg *config.Config) *Handler {
	return &Handler{Store: st, Chatwoot: cw, Config: cfg}
}

// Register registra as rotas de API do painel (protegidas por apikey) num
// *gin.Engine já existente. A UI é registrada separadamente por RegisterUI
// (ui.go), pois não depende de credenciais de acesso ao carregar a página.
func (h *Handler) Register(r *gin.Engine) {
	api := r.Group("/api")
	api.Use(h.authMiddleware)
	{
		api.GET("/instances", h.listInstances)
		api.GET("/evolution/instances", h.listEvolutionInstances)
		api.GET("/chatwoot/:instanceId", h.findChatwoot)
		api.POST("/chatwoot/:instanceId", h.setChatwoot)
		api.DELETE("/chatwoot/:instanceId", h.deleteChatwoot)
	}
}

// authMiddleware exige o header "apikey" igual a cfg.APIKey — mesmo
// esquema usado pelo evolutiongo para rotas administrativas (ver
// CONTRACTS.md item 2, auth via header apikey).
func (h *Handler) authMiddleware(c *gin.Context) {
	key := c.GetHeader("apikey")
	if h.Config == nil || h.Config.APIKey == "" || key != h.Config.APIKey {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "apikey inválida ou ausente"})
		return
	}
	c.Next()
}

// instanceSummary é o item retornado por GET /api/instances.
type instanceSummary struct {
	InstanceID   string `json:"instanceId"`
	InstanceName string `json:"instanceName"`
	Enabled      bool   `json:"enabled"`
	URL          string `json:"url"`
	NameInbox    string `json:"nameInbox"`
}

// listInstances lista as instâncias com config salva (habilitadas ou não).
// INTERFACE-CHANGE-REQUEST atendido: ListConfigs foi adicionado à interface
// Store pelo orquestrador.
func (h *Handler) listInstances(c *gin.Context) {
	cfgs, err := h.Store.ListConfigs(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	resp := make([]instanceSummary, 0, len(cfgs))
	for _, cfg := range cfgs {
		resp = append(resp, instanceSummary{
			InstanceID:   cfg.InstanceID,
			InstanceName: cfg.InstanceName,
			Enabled:      cfg.Enabled,
			URL:          cfg.URL,
			NameInbox:    cfg.NameInbox,
		})
	}
	c.JSON(http.StatusOK, resp)
}

// findResponse é a config + webhook_url calculada, como no controller antigo.
type findResponse struct {
	model.ChatwootConfig
	WebhookURL string `json:"webhook_url"`
}

// WebhookURL calcula a URL de webhook do inbox para uma instância, no
// mesmo formato do antigo: {SERVER_URL}/chatwoot/webhook/{instanceId}.
func WebhookURL(publicURL, instanceID string) string {
	return strings.TrimRight(publicURL, "/") + "/chatwoot/webhook/" + url.PathEscape(instanceID)
}

// findChatwoot implementa GET /api/chatwoot/:instanceId (= findChatwoot do
// controller antigo). Quando não há config salva, devolve um objeto vazio
// com enabled=false e webhook_url="" (mesmo comportamento do antigo quando
// Object.keys(result).length === 0).
func (h *Handler) findChatwoot(c *gin.Context) {
	instanceID := c.Param("instanceId")
	cfg, err := h.Store.GetConfig(c.Request.Context(), instanceID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if cfg == nil {
		c.JSON(http.StatusOK, findResponse{
			ChatwootConfig: model.ChatwootConfig{InstanceID: instanceID, Enabled: false},
			WebhookURL:     "",
		})
		return
	}
	c.JSON(http.StatusOK, findResponse{
		ChatwootConfig: *cfg,
		WebhookURL:     WebhookURL(h.Config.PublicURL, instanceID),
	})
}

// setRequest é o corpo aceito por POST /api/chatwoot/:instanceId.
// SignMsg é ponteiro para reproduzir a checagem do antigo
// (`data.signMsg !== true && data.signMsg !== false` -> obrigatório
// quando enabled=true), que em Go equivale a distinguir "não enviado" de
// "false".
type setRequest struct {
	InstanceName            string   `json:"instanceName"`
	Enabled                 bool     `json:"enabled"`
	URL                     string   `json:"url"`
	AccountID               string   `json:"accountId"`
	Token                   string   `json:"token"`
	NameInbox               string   `json:"nameInbox"`
	SignMsg                 *bool    `json:"signMsg"`
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
}

// isValidURL replica isURL(url, { require_tld: false }) da lib
// class-validator usada pelo controller antigo: exige apenas esquema
// http/https e host não vazio, sem exigir TLD (permite hosts internos
// como http://chatwoot).
func isValidURL(raw string) bool {
	if raw == "" {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	return u.Host != ""
}

// validateSet aplica as mesmas regras de createChatwoot() do controller
// antigo e monta o model.ChatwootConfig a persistir.
func validateSet(instanceID string, req *setRequest) (*model.ChatwootConfig, error) {
	if req.Enabled {
		if !isValidURL(req.URL) {
			return nil, errors.New("url is not valid")
		}
		if req.AccountID == "" {
			return nil, errors.New("accountId is required")
		}
		if req.Token == "" {
			return nil, errors.New("token is required")
		}
		if req.SignMsg == nil {
			return nil, errors.New("signMsg is required")
		}
		if !*req.SignMsg {
			req.SignDelimiter = ""
		}
	}

	nameInbox := req.NameInbox
	if nameInbox == "" {
		if req.InstanceName != "" {
			nameInbox = req.InstanceName
		} else {
			nameInbox = instanceID
		}
	}

	signMsg := false
	if req.SignMsg != nil {
		signMsg = *req.SignMsg
	}

	instanceName := req.InstanceName
	if instanceName == "" {
		instanceName = instanceID
	}

	return &model.ChatwootConfig{
		InstanceID:              instanceID,
		InstanceName:            instanceName,
		Enabled:                 req.Enabled,
		URL:                     req.URL,
		AccountID:               req.AccountID,
		Token:                   req.Token,
		NameInbox:               nameInbox,
		SignMsg:                 signMsg,
		SignDelimiter:           req.SignDelimiter,
		Number:                  req.Number,
		ReopenConversation:      req.ReopenConversation,
		ConversationPending:     req.ConversationPending,
		MergeBrazilContacts:     req.MergeBrazilContacts,
		ImportContacts:          req.ImportContacts,
		ImportMessages:          req.ImportMessages,
		DaysLimitImportMessages: req.DaysLimitImportMessages,
		AutoCreate:              req.AutoCreate,
		Organization:            req.Organization,
		Logo:                    req.Logo,
		IgnoreJids:              req.IgnoreJids,
	}, nil
}

// setChatwoot implementa POST /api/chatwoot/:instanceId (= createChatwoot
// do controller antigo): valida, salva e provisiona o inbox no Chatwoot
// quando enabled=true.
func (h *Handler) setChatwoot(c *gin.Context) {
	instanceID := c.Param("instanceId")

	var req setRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "corpo inválido: " + err.Error()})
		return
	}

	cfg, err := validateSet(instanceID, &req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := c.Request.Context()

	// Preserva o InboxID já provisionado (se houver) até o novo
	// provisionamento confirmar/atualizar o valor.
	if existing, err := h.Store.GetConfig(ctx, instanceID); err == nil && existing != nil {
		cfg.InboxID = existing.InboxID
	}

	if err := h.Store.SaveConfig(ctx, cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if err := h.provision(ctx, cfg); err != nil {
		// A config já foi salva; o erro de provisionamento é reportado
		// separadamente para o operador poder corrigir credenciais/URL do
		// Chatwoot sem perder o que já foi digitado no formulário.
		// 422 (e não 502): proxies como o Cloudflare substituem respostas
		// 502 do origin pela página de erro deles, escondendo o motivo do
		// operador no painel.
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "provisionamento Chatwoot falhou: " + err.Error()})
		return
	}

	// Persiste o InboxID resultante do provisionamento.
	if err := h.Store.SaveConfig(ctx, cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, findResponse{
		ChatwootConfig: *cfg,
		WebhookURL:     WebhookURL(h.Config.PublicURL, instanceID),
	})
}

// deleteChatwoot implementa DELETE /api/chatwoot/:instanceId. Não
// desprovisiona o inbox no Chatwoot (o antigo também não fazia isso no
// unset — apenas limpava a config local).
func (h *Handler) deleteChatwoot(c *gin.Context) {
	instanceID := c.Param("instanceId")
	if err := h.Store.DeleteConfig(c.Request.Context(), instanceID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}
