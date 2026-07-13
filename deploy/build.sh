#!/usr/bin/env bash
# build.sh — builda a imagem do evolution-chatwoot-connector.
#
# Equivale a rodar, a partir de connector/:
#   docker build --no-cache -t iceasa/evolution-chatwoot-connector:latest .
# (usando deploy/Dockerfile), e ao final imprime como atualizar o service
# em produção (via Portainer ou via linha de comando).
#
# Uso:
#   ./deploy/build.sh [tag]
#
# Se [tag] não for informado, usa "latest".
#
# Variáveis opcionais:
#   REGISTRY=meuregistry.example.com/iceasa   repositório da imagem (padrão: iceasa)
#   PUSH=1                                    também faz docker push (requer docker login)
set -euo pipefail

REGISTRY="${REGISTRY:-iceasa}"
IMAGE="${REGISTRY}/evolution-chatwoot-connector"
TAG="${1:-latest}"

# Diretório raiz do módulo Go (connector/), independente de onde o script for chamado.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECTOR_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

VERSION="${VERSION:-$(git -C "${CONNECTOR_DIR}" describe --tags --always --dirty 2>/dev/null || echo dev)}"

echo "==> Build (sem cache): ${IMAGE}:${TAG} (VERSION=${VERSION})"
docker build \
  --no-cache \
  --file "${SCRIPT_DIR}/Dockerfile" \
  --build-arg "VERSION=${VERSION}" \
  --tag "${IMAGE}:${TAG}" \
  "${CONNECTOR_DIR}"

if [[ "${PUSH:-0}" == "1" ]]; then
  echo "==> Push: ${IMAGE}:${TAG}"
  docker push "${IMAGE}:${TAG}"
fi

echo "==> Concluído: ${IMAGE}:${TAG}"

cat <<EOF

==============================================================
 Imagem buildada. Agora atualize o service em produção:
==============================================================


--- Via terminal --------------------------------------------

1. Descubra o nome do service do conector:

     docker service ls

   (procure pelo service com a imagem ${IMAGE})

2. Force a atualização para a imagem recém-buildada:

     docker service update --force --image ${IMAGE}:${TAG} <NOME-DO-SERVICE>

==============================================================
EOF
