package panel

import (
	"context"
	"fmt"

	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
)

// provision garante que a config habilitada tenha um inbox Chatwoot
// correspondente, com o webhook apontando para este conector.
//
// Reproduz initInstanceChatwoot() do serviço antigo
// (evolutionapi_antiga/.../services/chatwoot.service.ts): localizar (por
// nome) ou criar o inbox e garantir que o webhook_url do canal aponte para
// {PublicURL}/chatwoot/webhook/{instanceId}. No conector essas duas etapas
// já são expostas pelo chatwoot.Client (dono: Agente B) como:
//   - EnsureInbox: find-or-create do inbox pelo nome (cfg.NameInbox).
//   - UpdateInboxWebhook: garante que o webhook do canal está atualizado
//     (idempotente — útil tanto no fluxo normal quanto no autoCreate, que
//     no antigo é acionado após o QR code conectar e reaproveita o mesmo
//     inbox caso já exista um com o mesmo nome).
//
// Quando cfg.Enabled é false, nenhuma chamada é feita — a config apenas
// fica desativada localmente (mesmo comportamento do antigo, que só
// provisiona no Chatwoot quando enabled=true).
func (h *Handler) provision(ctx context.Context, cfg *model.ChatwootConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if h.Chatwoot == nil {
		return fmt.Errorf("cliente chatwoot não configurado")
	}

	webhookURL := WebhookURL(h.Config.PublicURL, cfg.InstanceID)

	inboxID, err := h.Chatwoot.EnsureInbox(ctx, cfg, webhookURL)
	if err != nil {
		return fmt.Errorf("ensure inbox: %w", err)
	}
	cfg.InboxID = inboxID

	// Garante que o webhook do inbox está atualizado mesmo quando o inbox
	// já existia (ex.: URL pública do conector mudou, ou reprovisionamento
	// via autoCreate reaproveitando um inbox pré-existente).
	if err := h.Chatwoot.UpdateInboxWebhook(ctx, cfg, inboxID, webhookURL); err != nil {
		return fmt.Errorf("update inbox webhook: %w", err)
	}

	return nil
}
