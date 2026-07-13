#!/bin/sh
# inject-menu.sh — entrypoint do service evolution_go que injeta o atalho do
# conector na dashboard (manager) ANTES de subir o servidor oficial.
#
# Roda a cada start do container => toda versão nova da imagem oficial é
# re-patchada automaticamente, sem fork e sem rebuild.
#
# Montado via Swarm configs (ver evolution-go-with-connector.yaml):
#   /patch/inject-menu.sh     (este script)
#   /patch/connector-menu.js  (o JS do menu)
set -eu

INDEX="/app/manager/dist/index.html"
ASSETS_DIR="/app/manager/dist/assets"
SRC_JS="/patch/connector-menu.js"
TAG='<script src="/assets/connector-menu.js"></script>'

if [ -f "$SRC_JS" ] && [ -d "$ASSETS_DIR" ]; then
  cp -f "$SRC_JS" "$ASSETS_DIR/connector-menu.js"
  # Injeção idempotente: só adiciona a tag se ainda não existir.
  if [ -f "$INDEX" ] && ! grep -q "connector-menu.js" "$INDEX"; then
    sed -i "s#</body>#${TAG}</body>#" "$INDEX"
  fi
  echo "[inject-menu] atalho do conector injetado na dashboard"
else
  echo "[inject-menu] AVISO: manager/dist não encontrado no layout esperado — pulando injeção (o manager segue intacto)"
fi

exec /app/server "$@"
