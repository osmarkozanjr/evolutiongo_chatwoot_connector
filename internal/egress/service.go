// service.go — orquestração do egress (Chatwoot → WhatsApp).
//
// Porta de receiveWebhook()/onSendMessageError() de
// evolutionapi_antiga/src/api/integrations/chatbot/chatwoot/services/chatwoot.service.ts
// usando as interfaces store.Store, chatwoot.Client e evolution.Client.
//
// INTERFACE-CHANGE-REQUEST: EvolutionInstanceToken(ctx, instanceID) (string, error)
//   Nenhuma interface fornece o token da instância do evolutiongo (header apikey)
//   no sentido egress — model.ChatwootConfig não o carrega e store.Store não o
//   expõe (no ingest ele chega no EventEnvelope.InstanceToken). Enquanto o
//   orquestrador não decidir onde persistir esse token, o Service recebe um
//   TokenResolver injetado no wire-up.
//
// INTERFACE-CHANGE-REQUEST: chatwoot.Client.CreateMessage(..., private bool)
//   onSendMessageError() no serviço antigo cria a mensagem de erro com
//   private: true. A interface atual não expõe o flag. Mitigação temporária:
//   enviamos a mensagem de erro com source_id prefixado "WAID:" para que o
//   próprio webhook a ignore como echo (mesmo mecanismo de dedup do antigo).
//
// INTERFACE-CHANGE-REQUEST: evolution.Client.DeleteMessage(...)
//   Para message_updated com content_attributes.deleted o antigo envia a
//   revogação (sendMessage {delete: key}). O evolutiongo TEM o endpoint
//   POST /message/delete (verificado em routes.go → DeleteMessageEveryone),
//   mas evolution.Client não o expõe; sem mudança de interface só registramos.
package egress

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/iceasa/evolution-chatwoot-connector/internal/chatwoot"
	"github.com/iceasa/evolution-chatwoot-connector/internal/evolution"
	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
	"github.com/iceasa/evolution-chatwoot-connector/internal/store"
)

// Strings de erro portadas de utils/translations/pt-BR.json do serviço antigo.
const (
	msgNumberNotInWhatsapp = "🚨 A mensagem não foi enviada, pois o contato não é um número válido do WhatsApp."
	msgNotSentPrefix       = "🚨 Não foi possível enviar a mensagem. Verifique sua conexão."
)

// errorSourceID marca mensagens de erro criadas pelo próprio conector para que
// o webhook as ignore (mesmo prefixo "WAID:" usado no dedup do antigo).
const errorSourceID = "WAID:connector-error"

// botContactPhone é o contato bot da integração antiga ('123456').
const botContactPhone = "123456"

// TokenResolver resolve o token (header apikey) da instância do evolutiongo.
type TokenResolver func(ctx context.Context, instanceID string) (string, error)

// Service orquestra o processamento do webhook do Chatwoot.
type Service struct {
	store  store.Store
	cw     chatwoot.Client
	evo    evolution.Client
	tokens TokenResolver
	log    *slog.Logger
}

// NewService monta o Service. logger pode ser nil (usa slog.Default()).
func NewService(st store.Store, cw chatwoot.Client, evo evolution.Client, tokens TokenResolver, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{store: st, cw: cw, evo: evo, tokens: tokens, log: logger}
}

// HandleWebhook processa um evento do webhook do Chatwoot para a instância.
// A ordem dos checks segue receiveWebhook() do serviço antigo.
func (s *Service) HandleWebhook(ctx context.Context, instanceID string, p *WebhookPayload) error {
	cfg, err := s.store.GetConfig(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("egress: load config %s: %w", instanceID, err)
	}
	if cfg == nil || !cfg.Enabled {
		// Sem config habilitada: ignora silenciosamente (paridade com
		// provider not found no antigo).
		return nil
	}

	// Porta: reopenConversation===false + conversation_status_changed=resolved
	// invalida o cache conversa↔contato para que a próxima mensagem crie uma
	// conversa nova. Nosso equivalente persistente é remover o mapping.
	if p.Event == "conversation_status_changed" && p.Status == "resolved" && !cfg.ReopenConversation {
		if jid := p.metaSenderIdentifier(); jid != "" {
			if err := s.store.DeleteConversation(ctx, instanceID, jid); err != nil {
				s.log.Warn("egress: delete conversation mapping", "instanceId", instanceID, "remoteJid", jid, "err", err)
			}
		}
	}

	// Early-returns idênticos ao antigo: sem conversation no payload, private
	// notes, ou message_updated que não seja deleção.
	if p.Conversation == nil || p.Private ||
		(p.Event == "message_updated" && (p.ContentAttributes == nil || !p.ContentAttributes.Deleted)) {
		return nil
	}

	chatID := p.chatID()

	// message_updated + deleted: o antigo envia a revogação para o WhatsApp
	// (sendMessage {delete: key}) e apaga o registro local.
	if p.Event == "message_updated" && p.ContentAttributes != nil && p.ContentAttributes.Deleted {
		mapping, err := s.store.GetMessageByChatwootID(ctx, instanceID, p.ID)
		if err != nil {
			return fmt.Errorf("egress: lookup deleted message %d: %w", p.ID, err)
		}
		if mapping != nil {
			// TODO(VERIFY): revogação não enviada — o evolutiongo expõe
			// POST /message/delete (routes.go: DeleteMessageEveryone), mas
			// evolution.Client não tem método para isso. Ver
			// INTERFACE-CHANGE-REQUEST no topo do arquivo.
			s.log.Warn("egress: message deleted in chatwoot; revoke not sent (interface gap)",
				"instanceId", instanceID, "chatwootMessageId", p.ID, "waMessageId", mapping.WhatsappMessageID)
		}
		return nil
	}

	// Contato bot '123456': o antigo trata comandos de conexão (/init,
	// /iniciar, status, disconnect...) que operam a sessão do WhatsApp.
	// O conector não gerencia a sessão do evolutiongo (interface não expõe
	// /instance/*), então apenas ignoramos. TODO(VERIFY): decidir se o painel
	// do conector cobre esse fluxo; não inventamos chamadas aqui.
	if chatID == botContactPhone {
		return nil
	}

	// Chatwoot → WhatsApp: mensagens outgoing criadas por agentes.
	if p.MessageType == "outgoing" && len(p.Conversation.Messages) > 0 {
		return s.handleOutgoing(ctx, instanceID, cfg, p, chatID)
	}

	// Porta do bloco de template: envia o texto cru com quebras normalizadas.
	if p.MessageType == "template" && p.Event == "message_created" && chatID != "" {
		token, err := s.tokens(ctx, instanceID)
		if err != nil {
			return fmt.Errorf("egress: resolve token %s: %w", instanceID, err)
		}
		_, err = s.evo.SendText(ctx, token, &evolution.TextMessage{
			Number: chatID,
			Text:   normalizeTemplateText(p.Content),
		})
		if err != nil {
			return fmt.Errorf("egress: send template: %w", err)
		}
	}

	return nil
}

// handleOutgoing envia a(s) mensagem(ns) outgoing para o WhatsApp.
func (s *Service) handleOutgoing(ctx context.Context, instanceID string, cfg *model.ChatwootConfig, p *WebhookPayload, chatID string) error {
	// Echo: mensagem criada pelo próprio conector (ingest WA→CW grava
	// source_id "WAID:{key.id}"; mensagens de erro usam o mesmo prefixo).
	if strings.HasPrefix(p.Conversation.Messages[0].SourceID, "WAID:") {
		return nil
	}
	// Dedup extra pelo mapping persistido (mensagem já correlacionada).
	if m, err := s.store.GetMessageByChatwootID(ctx, instanceID, p.ID); err == nil && m != nil {
		return nil
	}

	if chatID == "" {
		s.onSendMessageError(ctx, cfg, p.Conversation.ID, fmt.Errorf("conversa sem identifier/phone_number"))
		return fmt.Errorf("egress: conversation %d sem remoteJid no payload", p.Conversation.ID)
	}

	token, err := s.tokens(ctx, instanceID)
	if err != nil {
		s.onSendMessageError(ctx, cfg, p.Conversation.ID, err)
		return fmt.Errorf("egress: resolve token %s: %w", instanceID, err)
	}

	formatText := formatOutgoingText(p.Content, p.senderName(), cfg.SignMsg, cfg.SignDelimiter)

	for _, msg := range p.Conversation.Messages {
		if len(msg.Attachments) > 0 {
			// Sem content, anexo vai sem caption (formatText = null no antigo).
			caption := formatText
			if p.Content == "" {
				caption = ""
			}
			for _, att := range msg.Attachments {
				mediaType, filename := deduceAttachment(att.DataURL)
				res, err := s.evo.SendMedia(ctx, token, &evolution.MediaMessage{
					Number:   chatID,
					URL:      att.DataURL,
					Type:     mediaType,
					Caption:  caption,
					Filename: filename,
				})
				if err != nil {
					s.onSendMessageError(ctx, cfg, p.Conversation.ID, err)
					return fmt.Errorf("egress: send media: %w", err)
				}
				s.saveOutMapping(ctx, instanceID, p.ID, res)
			}
		} else {
			res, err := s.evo.SendText(ctx, token, &evolution.TextMessage{
				Number: chatID,
				Text:   formatText,
			})
			if err != nil {
				s.onSendMessageError(ctx, cfg, p.Conversation.ID, err)
				return fmt.Errorf("egress: send text: %w", err)
			}
			s.saveOutMapping(ctx, instanceID, p.ID, res)
		}
	}

	return nil
}

// saveOutMapping persiste o MessageMapping direction=out (correlação/dedup).
func (s *Service) saveOutMapping(ctx context.Context, instanceID string, cwMessageID int, res *evolution.SendResult) {
	if res == nil || res.MessageID == "" {
		s.log.Warn("egress: send result sem message id; mapping não salvo",
			"instanceId", instanceID, "chatwootMessageId", cwMessageID)
		return
	}
	m := &model.MessageMapping{
		InstanceID:        instanceID,
		WhatsappMessageID: res.MessageID,
		ChatwootMessageID: cwMessageID,
		Direction:         "out",
		CreatedAt:         time.Now(),
	}
	if err := s.store.SaveMessage(ctx, m); err != nil {
		s.log.Warn("egress: save message mapping", "instanceId", instanceID, "err", err)
	}
}

// onSendMessageError porta onSendMessageError(): cria mensagem de erro na
// conversa do Chatwoot quando o envio ao WhatsApp falha.
// No antigo a mensagem é private:true; ver INTERFACE-CHANGE-REQUEST no topo —
// por ora usamos source_id "WAID:..." para o webhook ignorar o echo.
func (s *Service) onSendMessageError(ctx context.Context, cfg *model.ChatwootConfig, conversationID int, sendErr error) {
	if conversationID == 0 {
		return
	}

	content := fmt.Sprintf("%s _%v_", msgNotSentPrefix, sendErr)
	// O antigo diferencia "número não existe no WhatsApp" (erro 400 exists=false
	// da Baileys). No evolutiongo o erro equivalente é a string
	// "is not registered on WhatsApp" (send_service.go).
	if sendErr != nil && strings.Contains(sendErr.Error(), "is not registered on WhatsApp") {
		content = msgNumberNotInWhatsapp
	}

	if _, err := s.cw.CreateMessage(ctx, cfg, conversationID, content, "outgoing", nil, errorSourceID); err != nil {
		s.log.Warn("egress: create error message on chatwoot", "conversationId", conversationID, "err", err)
	}
}
