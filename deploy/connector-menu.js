/**
 * connector-menu.js — injeta a integração Chatwoot na dashboard (manager) do
 * evolutiongo SEM fork: copiado para manager/dist/assets/ e referenciado no
 * index.html pelo inject-menu.sh a cada start do container, sobrevivendo a
 * qualquer update da imagem oficial.
 *
 * Duas injeções (SPA-aware — o manager é React com client-side routing):
 *
 *   1. CARD "Chatwoot" na página de configurações da instância
 *      (/manager/instances/<id>/settings): um card com o MESMO estilo dos
 *      demais (classes extraídas do bundle oficial: rounded-lg border
 *      border-sidebar-border bg-card p-6 / h2 text-lg font-semibold), com
 *      botão que abre o painel do conector JÁ naquela instância.
 *
 *   2. Item "Chatwoot" na sidebar — SOMENTE dentro do contexto de uma
 *      instância (/manager/instances/<id>/...). Na raiz (lista de instâncias,
 *      dashboard) o item é removido; o href é sempre contextual.
 *
 * Defensivo: se o upstream mudar o layout, nada quebra — o card simplesmente
 * não encontra âncora e a sidebar cai para um botão flutuante.
 */
(function () {
  "use strict";

  var SIDEBAR_ID = "evo-chatwoot-connector-link";
  var CARD_ID = "evo-chatwoot-instance-card";
  var INSTANCE_RE = /\/manager\/instances\/([0-9a-fA-F-]{36})(?:\/|$)/;

  function currentInstanceId() {
    var m = window.location.pathname.match(INSTANCE_RE);
    return m ? m[1] : null;
  }

  function connectorHref() {
    var id = currentInstanceId();
    return id ? "/connector/?instance=" + encodeURIComponent(id) : "/connector/";
  }

  // ---------- 1. Card na página de settings da instância ----------

  function onSettingsPage() {
    return /\/manager\/instances\/[0-9a-fA-F-]{36}\/settings/.test(window.location.pathname);
  }

  function tryInstanceCard() {
    var existing = document.getElementById(CARD_ID);

    if (!onSettingsPage()) {
      // Saiu da página (SPA): remove o card para não vazar para outras telas.
      if (existing) existing.remove();
      return;
    }
    if (existing) return;

    // Âncora: os cards da página usam bg-card (ex.: "Configurações de Webhook").
    var cards = document.querySelectorAll("div.bg-card");
    if (cards.length === 0) return; // página ainda montando — tenta no próximo tick
    var anchor = cards[cards.length - 1];

    var id = currentInstanceId();
    var card = document.createElement("div");
    card.id = CARD_ID;
    card.className = anchor.className; // mesmo estilo dos cards oficiais

    var title = document.createElement("h2");
    title.className = "text-lg font-semibold text-foreground mb-4";
    title.textContent = "Chatwoot";

    var desc = document.createElement("p");
    desc.className = "text-sm text-muted-foreground mb-4";
    desc.textContent = "Integração desta instância com o Chatwoot (conversas do WhatsApp como inbox).";

    var btn = document.createElement("a");
    btn.href = "/connector/?instance=" + encodeURIComponent(id);
    btn.target = "_blank";
    btn.rel = "noopener";
    // Mesmas classes dos botões primários do manager (extraídas do bundle).
    btn.className = "inline-flex items-center justify-center gap-2 rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90";
    btn.textContent = "Configurar Chatwoot";

    card.appendChild(title);
    card.appendChild(desc);
    card.appendChild(btn);
    anchor.parentElement.appendChild(card);
  }

  // ---------- 2. Item na sidebar global ----------

  function trySidebar() {
    if (document.getElementById(SIDEBAR_ID)) return true;

    var containers = document.querySelectorAll("nav, aside, [class*='sidebar' i], [class*='menu' i]");
    for (var i = 0; i < containers.length; i++) {
      var sample = containers[i].querySelector("a[href]");
      if (!sample) continue;

      var link = document.createElement("a");
      link.id = SIDEBAR_ID;
      link.href = connectorHref();
      link.target = "_blank";
      link.rel = "noopener";
      link.className = sample.className;
      link.textContent = "Chatwoot";
      link.title = "Integração Chatwoot (conector)";
      sample.parentElement.appendChild(link);
      return true;
    }
    return false;
  }

  function floatingButton() {
    if (document.getElementById(SIDEBAR_ID)) return;
    var btn = document.createElement("a");
    btn.id = SIDEBAR_ID;
    btn.href = connectorHref();
    btn.target = "_blank";
    btn.rel = "noopener";
    btn.textContent = "💬 Chatwoot";
    btn.style.cssText = [
      "position:fixed", "right:16px", "bottom:16px", "z-index:99999",
      "background:#0ea47a", "color:#fff", "padding:10px 16px",
      "border-radius:999px", "font:600 13px/1 sans-serif",
      "text-decoration:none", "box-shadow:0 4px 14px rgba(0,0,0,.35)",
    ].join(";");
    document.body.appendChild(btn);
  }

  // ---------- Loop de manutenção (SPA re-renderiza a qualquer momento) ----------

  function maintain() {
    tryInstanceCard();

    var link = document.getElementById(SIDEBAR_ID);

    // Sidebar só dentro do contexto de uma instância; na raiz, remove.
    if (!currentInstanceId()) {
      if (link) link.remove();
      maintain.attempts = 0;
      return;
    }

    if (!link) {
      if (!trySidebar()) {
        if (maintain.attempts > 15) floatingButton();
        maintain.attempts++;
      }
    } else {
      var href = connectorHref();
      if (link.getAttribute("href") !== href) link.setAttribute("href", href);
      link.title = "Chatwoot desta instância";
    }
  }
  maintain.attempts = 0;

  function start() {
    maintain();
    setInterval(maintain, 1000);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
