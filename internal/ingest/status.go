// status.go — eventos de conexão/QR → mensagens de bot no Chatwoot.
//
// Porta de createBotMessage()/createBotQr() e dos blocos connection.update /
// qrcode.updated / status.instance de eventWhatsapp() (chatwoot.service.ts).
// O contato "bot" (telefone 123456) é criado no provisionamento do inbox
// (initInstanceChatwoot — responsabilidade do painel/Agente D); aqui apenas o
// localizamos. Sem contato/conversa do bot, os eventos são ignorados com log
// (mesmo comportamento do serviço antigo).
//
// Payloads verificados em evolutiongo_original/pkg/whatsmeow/service/whatsmeow.go:
//   - Connected:    data = { "status": "open", "jid": "...", "pushName": "..." }
//   - QRCode:       data = { "qrcode": "data:image/png;base64,...", "code": "...",
//                            "count": N, "maxCount": M }
//   - QRTimeout:    data = {} ou { "reason": "...", "qrcount": N, "maxCount": M,
//                            "forceLogout": bool }
//   - LoggedOut:    data = { "reason": "Logged out" } (caminho do StartClient) ou
//                   events.LoggedOut marshalado (chaves prováveis "OnConnect",
//                   "Reason" — TODO(VERIFY): sem as tags JSON do whatsmeow no
//                   workspace, aceitamos "reason"/"Reason").
//   - Disconnected: data = events.Disconnected marshalado (struct sem campos
//                   relevantes; usamos texto fixo).
package ingest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/iceasa/evolution-chatwoot-connector/internal/chatwoot"
	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
)

// botContactPhone é o telefone do contato bot criado pelo provisionamento
// (initInstanceChatwoot usava '123456').
const botContactPhone = "+123456"

// connNotifyThrottle reproduz o throttling de 30s do connection.update antigo.
const connNotifyThrottle = 30 * time.Second

func (s *Service) handleConnected(ctx context.Context, cfg *model.ChatwootConfig, ev *model.EventEnvelope) error {
	// Throttle por instância (porta do lastConnectionNotification).
	s.connMu.Lock()
	last := s.connNotified[cfg.InstanceID]
	if time.Since(last) < connNotifyThrottle {
		s.connMu.Unlock()
		s.log.Debug("ingest: notificação de conexão suprimida (throttle)", "instanceId", cfg.InstanceID)
		return nil
	}
	s.connNotified[cfg.InstanceID] = time.Now()
	s.connMu.Unlock()

	msg := "🚀 Conectado ao WhatsApp com sucesso!"
	var data map[string]any
	if err := json.Unmarshal(ev.Data, &data); err == nil {
		if pn := getString(data, "pushName", "PushName"); pn != "" {
			msg = fmt.Sprintf("🚀 %s conectado ao WhatsApp com sucesso!", pn)
		}
	}
	return s.createBotMessage(ctx, cfg, msg, nil)
}

func (s *Service) handleDisconnected(ctx context.Context, cfg *model.ChatwootConfig, ev *model.EventEnvelope) error {
	msg := fmt.Sprintf("⚠️ Instância %s desconectada do WhatsApp.", displayName(cfg, ev))
	return s.createBotMessage(ctx, cfg, msg, nil)
}

func (s *Service) handleLoggedOut(ctx context.Context, cfg *model.ChatwootConfig, ev *model.EventEnvelope) error {
	reason := ""
	var data map[string]any
	if err := json.Unmarshal(ev.Data, &data); err == nil {
		reason = getString(data, "reason", "Reason")
	}
	msg := fmt.Sprintf("🚨 Instância %s: sessão do WhatsApp encerrada (logout).", displayName(cfg, ev))
	if reason != "" {
		msg += " Motivo: " + reason
	}
	return s.createBotMessage(ctx, cfg, msg, nil)
}

func (s *Service) handleQRCode(ctx context.Context, cfg *model.ChatwootConfig, ev *model.EventEnvelope) error {
	var data map[string]any
	if err := json.Unmarshal(ev.Data, &data); err != nil {
		s.log.Warn("ingest: payload de qrcode inválido", "instanceId", ev.InstanceID, "error", err)
		return nil
	}

	// data.qrcode = data URI "data:image/png;base64,<...>" (verificado).
	qr := getString(data, "qrcode")
	raw := qr
	if i := strings.Index(raw, "base64,"); i >= 0 {
		raw = raw[i+len("base64,"):]
	}

	var attachments []chatwoot.Attachment
	if raw != "" {
		if png, err := base64.StdEncoding.DecodeString(raw); err == nil {
			attachments = append(attachments, chatwoot.Attachment{
				Filename: displayName(cfg, ev) + ".png",
				Mime:     "image/png",
				Data:     png,
			})
		} else {
			s.log.Warn("ingest: base64 do qrcode inválido", "instanceId", ev.InstanceID, "error", err)
		}
	}

	// Igual ao antigo: uma mensagem com o anexo do QR e outra com a instrução.
	if len(attachments) > 0 {
		if err := s.createBotMessage(ctx, cfg, "QRCode gerado com sucesso!", attachments); err != nil {
			return err
		}
	}
	return s.createBotMessage(ctx, cfg, "⚡️QRCode gerado com sucesso!\n\nEscaneie o QRCode para conectar.", nil)
}

func (s *Service) handleQRTimeout(ctx context.Context, cfg *model.ChatwootConfig, ev *model.EventEnvelope) error {
	reason := ""
	var data map[string]any
	if err := json.Unmarshal(ev.Data, &data); err == nil {
		reason = getString(data, "reason")
	}
	msg := "🚨 Limite de geração de QRCode atingido. Para gerar um novo QRCode, solicite uma nova conexão da instância."
	if reason == "" {
		msg = "🚨 O QRCode expirou sem ser escaneado. Solicite uma nova conexão para gerar outro."
	}
	return s.createBotMessage(ctx, cfg, msg, nil)
}

// createBotMessage porta createBotMessage()/createBotQr(): localiza o contato
// bot (123456) e a conversa aberta dele no inbox da instância e cria a
// mensagem lá. Ausência de contato/conversa não é erro (retorna nil com log),
// como no serviço antigo.
func (s *Service) createBotMessage(ctx context.Context, cfg *model.ChatwootConfig, content string, attachments []chatwoot.Attachment) error {
	if cfg.InboxID == 0 {
		s.log.Warn("ingest: bot message ignorada — config sem inboxId", "instanceId", cfg.InstanceID)
		return nil
	}

	contact, err := s.cw.SearchContact(ctx, cfg, botContactPhone)
	if err != nil {
		return fmt.Errorf("ingest: erro ao buscar contato bot: %w", err)
	}
	if contact == nil {
		s.log.Warn("ingest: contato bot (123456) não encontrado — provisionamento do inbox pendente?", "instanceId", cfg.InstanceID)
		return nil
	}

	conv, err := s.cw.GetOpenConversation(ctx, cfg, contact.ID, cfg.InboxID)
	if err != nil {
		return fmt.Errorf("ingest: erro ao buscar conversa do bot: %w", err)
	}
	if conv == nil {
		s.log.Warn("ingest: conversa aberta do bot não encontrada", "instanceId", cfg.InstanceID)
		return nil
	}

	if _, err := s.cw.CreateMessage(ctx, cfg, conv.ID, content, "incoming", attachments, ""); err != nil {
		return fmt.Errorf("ingest: erro ao criar mensagem de bot: %w", err)
	}
	return nil
}

func displayName(cfg *model.ChatwootConfig, ev *model.EventEnvelope) string {
	switch {
	case ev != nil && ev.InstanceName != "":
		return ev.InstanceName
	case cfg.InstanceName != "":
		return cfg.InstanceName
	default:
		return cfg.InstanceID
	}
}
