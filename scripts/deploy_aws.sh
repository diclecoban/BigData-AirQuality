#!/usr/bin/env bash
# deploy_aws.sh — BigData-AirQuality tam pipeline kurulumu (Ubuntu 22.04 EC2)
#
# Kullanım (EC2 üzerinde):
#   chmod +x scripts/deploy_aws.sh
#   OPENAQ_API_KEY=<key> bash scripts/deploy_aws.sh
#
# Opsiyonel değişkenler:
#   FETCH_START   : veri çekme başlangıç tarihi (default: 2024-01-01)
#   FETCH_END     : veri çekme bitiş tarihi    (default: 2024-12-31)
#   FETCH_SOURCE  : ibb | openaq | both        (default: both)
#   SKIP_FETCH    : 1 → veri çekmeyi atla     (default: 0)
#   SKIP_TRAIN    : 1 → model eğitimini atla  (default: 0)

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
die()     { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA_DIR="$REPO_DIR/infra"

FETCH_START="${FETCH_START:-2024-01-01}"
FETCH_END="${FETCH_END:-2024-12-31}"
FETCH_SOURCE="${FETCH_SOURCE:-both}"
SKIP_FETCH="${SKIP_FETCH:-0}"
SKIP_TRAIN="${SKIP_TRAIN:-0}"

# ─── 1. EC2 Public IP ────────────────────────────────────────────────────────
info "EC2 public IP alınıyor..."
TOKEN=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || true)
if [[ -n "$TOKEN" ]]; then
    EC2_PUBLIC_IP=$(curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" \
        http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || true)
fi
if [[ -z "${EC2_PUBLIC_IP:-}" ]]; then
    warn "Metadata servisine ulaşılamadı, genel IP manuel giriliyor..."
    read -rp "EC2 Public IP: " EC2_PUBLIC_IP
fi
export EC2_PUBLIC_IP
success "EC2 IP: $EC2_PUBLIC_IP"

# ─── 2. Docker kurulumu (yoksa) ──────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
    info "Docker kuruluyor..."
    sudo apt-get update -qq
    sudo apt-get install -y -qq ca-certificates curl gnupg lsb-release
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
        | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update -qq
    sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin
    sudo usermod -aG docker "$USER"
    success "Docker kuruldu."
else
    success "Docker zaten kurulu: $(docker --version)"
fi

if ! docker compose version &>/dev/null; then
    die "docker compose plugin bulunamadı. Manuel kontrol et."
fi

# ─── 3. Python bağımlılıkları (venv) ─────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    info "Python3 kuruluyor..."
    sudo apt-get install -y -qq python3 python3-venv
fi
if ! python3 -c "import venv" &>/dev/null; then
    sudo apt-get install -y -qq python3-venv
fi

VENV_DIR="$HOME/airquality-venv"
if [[ ! -d "$VENV_DIR" ]]; then
    info "Virtual environment oluşturuluyor: $VENV_DIR"
    python3 -m venv "$VENV_DIR"
fi
# Tüm python/pip komutları venv üzerinden çalışır
PYTHON="$VENV_DIR/bin/python"
PIP="$VENV_DIR/bin/pip"

info "Python bağımlılıkları yükleniyor..."
"$PIP" install -q --upgrade pip
"$PIP" install -q pandas requests python-dotenv numpy pyspark==3.5.8 mlflow
success "Python bağımlılıkları yüklendi."

# ─── 4. .env dosyası ─────────────────────────────────────────────────────────
ENV_FILE="$REPO_DIR/.env"
if [[ ! -f "$ENV_FILE" ]]; then
    info ".env dosyası oluşturuluyor..."
    cat > "$ENV_FILE" <<EOF
OPENAQ_API_KEY=${OPENAQ_API_KEY:-}
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SPARK_MASTER=spark://localhost:7077
MLFLOW_TRACKING_URI=http://localhost:5001
EOF
    success ".env oluşturuldu."
else
    # OPENAQ_API_KEY güncelle
    if [[ -n "${OPENAQ_API_KEY:-}" ]]; then
        sed -i "s|^OPENAQ_API_KEY=.*|OPENAQ_API_KEY=${OPENAQ_API_KEY}|" "$ENV_FILE" \
            || echo "OPENAQ_API_KEY=${OPENAQ_API_KEY}" >> "$ENV_FILE"
    fi
    success ".env mevcut, güncellendi."
fi

if grep -q "^OPENAQ_API_KEY=$" "$ENV_FILE" 2>/dev/null; then
    warn "OPENAQ_API_KEY boş! Sadece IBB kaynağı kullanılacak."
    FETCH_SOURCE="ibb"
fi

# ─── 5. Docker Compose ile servisleri başlat ─────────────────────────────────
info "Servisler başlatılıyor (Kafka, Spark, MLflow, Grafana)..."
cd "$INFRA_DIR"

docker compose -f docker-compose.yml -f docker-compose.aws.yml down --remove-orphans 2>/dev/null || true
docker compose -f docker-compose.yml -f docker-compose.aws.yml up -d

# Kafka sağlık kontrolü
info "Kafka hazır olana kadar bekleniyor..."
for i in $(seq 1 30); do
    if docker exec airquality-kafka kafka-topics \
        --bootstrap-server localhost:9092 --list &>/dev/null 2>&1; then
        success "Kafka hazır."
        break
    fi
    if [[ $i -eq 30 ]]; then
        die "Kafka 60 saniyede başlamadı. 'docker logs airquality-kafka' ile kontrol et."
    fi
    sleep 2
done

# Tüm servislerin başladığını bekle
sleep 10
success "Tüm servisler başlatıldı."

# ─── 6. Gerçek veri çekme ─────────────────────────────────────────────────────
cd "$REPO_DIR"
if [[ "$SKIP_FETCH" == "1" ]]; then
    warn "SKIP_FETCH=1 — veri çekme atlandı."
else
    info "Gerçek AQ verisi çekiliyor ($FETCH_START → $FETCH_END, kaynak: $FETCH_SOURCE)..."
    "$PYTHON" scripts/fetch_real_airquality.py \
        --start-date "$FETCH_START" \
        --end-date   "$FETCH_END" \
        --source     "$FETCH_SOURCE"
    success "Veri çekme tamamlandı."
fi

# ─── 7. Model eğitimi ─────────────────────────────────────────────────────────
if [[ "$SKIP_TRAIN" == "1" ]]; then
    warn "SKIP_TRAIN=1 — model eğitimi atlandı."
else
    export SPARK_MASTER="local[*]"
    export MLFLOW_TRACKING_URI="http://localhost:5001"

    info "Baseline modeller eğitiliyor..."
    "$PYTHON" -m src.ml.train_baseline_models
    success "Baseline modeller tamamlandı."

    info "GBT modeller eğitiliyor..."
    "$PYTHON" -m src.ml.train_gbt_model
    success "GBT modeller tamamlandı."

    info "Modeller değerlendiriliyor..."
    "$PYTHON" -m src.ml.evaluate_models
    success "Değerlendirme tamamlandı."

    echo ""
    echo "━━━ Sonuçlar ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    "$PYTHON" -c "
import pandas as pd
df = pd.read_csv('data/reports/evaluation_summary.csv')
print(df.to_string(index=False))
"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
fi

# ─── 8. Özet ──────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}━━━ Pipeline Hazır ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "  Grafana   → http://${EC2_PUBLIC_IP}:3000  (admin / admin)"
echo -e "  MLflow    → http://${EC2_PUBLIC_IP}:5001"
echo -e "  Spark UI  → http://${EC2_PUBLIC_IP}:8080"
echo -e "  Kafka     → ${EC2_PUBLIC_IP}:9092"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "Servisleri durdurmak için:"
echo "  cd infra && docker compose -f docker-compose.yml -f docker-compose.aws.yml down"
