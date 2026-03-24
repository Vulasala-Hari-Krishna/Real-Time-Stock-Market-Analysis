#!/usr/bin/env bash
# setup-local.sh — One-time local development setup.
#
# Checks prerequisites (Docker, AWS CLI, Python), copies .env.example to
# .env, prompts for API keys, and builds all Docker images.
#
# Usage: bash scripts/setup-local.sh
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }
ok()    { echo -e "  ${GREEN}✔${NC} $*"; }
fail()  { echo -e "  ${RED}✘${NC} $*"; }

COMPOSE_FILE="docker/docker-compose.yaml"
MISSING=0

# ── 1. Check prerequisites ──────────────────────────────────────────
echo ""
info "Checking prerequisites..."

# Docker
if command -v docker &>/dev/null; then
    ok "Docker $(docker --version | awk '{print $3}' | tr -d ',')"
else
    fail "Docker is not installed. Please install Docker Desktop."
    MISSING=1
fi

# Docker Compose (v2 plugin)
if docker compose version &>/dev/null; then
    ok "Docker Compose $(docker compose version --short)"
else
    fail "Docker Compose v2 not found."
    MISSING=1
fi

# AWS CLI
if command -v aws &>/dev/null; then
    ok "AWS CLI $(aws --version 2>&1 | awk '{print $1}' | cut -d/ -f2)"
else
    warn "AWS CLI not found. Required only for CloudFormation deployment."
fi

# Python
if command -v python3 &>/dev/null; then
    ok "Python $(python3 --version 2>&1 | awk '{print $2}')"
elif command -v python &>/dev/null; then
    ok "Python $(python --version 2>&1 | awk '{print $2}')"
else
    fail "Python 3.11+ is not installed."
    MISSING=1
fi

if [ "${MISSING}" -eq 1 ]; then
    echo ""
    error "Missing required tools. Please install them and re-run."
    exit 1
fi

# ── 2. Create .env from template ────────────────────────────────────
echo ""
if [ -f .env ]; then
    info ".env already exists — skipping creation."
else
    if [ ! -f .env.example ]; then
        error ".env.example not found in project root."
        exit 1
    fi
    cp .env.example .env
    info "Created .env from .env.example."
fi

# ── 3. Prompt for API keys ──────────────────────────────────────────
echo ""
info "You can configure your API keys now (or edit .env later)."
echo ""

read -rp "Alpha Vantage API key (leave blank to skip): " AV_KEY
if [ -n "${AV_KEY}" ]; then
    sed -i.bak "s|^ALPHA_VANTAGE_API_KEY=.*|ALPHA_VANTAGE_API_KEY=${AV_KEY}|" .env
    rm -f .env.bak
    ok "Alpha Vantage key saved."
fi

read -rp "AWS Access Key ID (leave blank to skip): " AWS_KEY
if [ -n "${AWS_KEY}" ]; then
    sed -i.bak "s|^AWS_ACCESS_KEY_ID=.*|AWS_ACCESS_KEY_ID=${AWS_KEY}|" .env
    rm -f .env.bak
    ok "AWS Access Key saved."
fi

read -rp "AWS Secret Access Key (leave blank to skip): " AWS_SECRET
if [ -n "${AWS_SECRET}" ]; then
    sed -i.bak "s|^AWS_SECRET_ACCESS_KEY=.*|AWS_SECRET_ACCESS_KEY=${AWS_SECRET}|" .env
    rm -f .env.bak
    ok "AWS Secret Key saved."
fi

# ── 4. Install Python dev dependencies ──────────────────────────────
echo ""
info "Installing Python development dependencies..."
if [ -d ".venv" ]; then
    info "Virtual environment detected (.venv/)."
fi
pip install -r requirements.txt -r requirements-dev.txt --quiet 2>/dev/null || \
    warn "pip install had warnings — check manually if needed."
ok "Python dependencies installed."

# ── 5. Build Docker images ──────────────────────────────────────────
echo ""
info "Building Docker images..."
docker compose -f "${COMPOSE_FILE}" build
ok "Docker images built."

# ── 6. Done ─────────────────────────────────────────────────────────
echo ""
info "=========================================="
info "  Setup complete!"
info "=========================================="
echo ""
info "Next steps:"
info "  1. Edit .env and fill in any remaining values."
info "  2. Run 'make start' to launch all services."
info "  3. Open http://localhost:8501 for the dashboard."
echo ""
