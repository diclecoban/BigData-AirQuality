# AWS Deployment Kılavuzu

## Gereksinimler
- AWS hesabı
- `.pem` key dosyası (EC2 oluştururken indirildi)
- OpenAQ API key (https://api.openaq.org adresinden alınır)

---

## 1. EC2 Instance Oluşturma

1. AWS Console → **EC2 → Launch Instance**
2. **AMI**: Ubuntu Server 22.04 veya 26.04 LTS
3. **Instance type**: `t3.xlarge` (4 vCPU, 16 GB RAM) — önerilen  
   `t3.large` (2 vCPU, 8 GB) da çalışır ama yavaş olabilir
4. **Key pair**: Yeni oluştur, `.pem` formatında indir, kaybet
5. **Security Group** — şu inbound kurallarını ekle:

   | Port | Protokol | Kaynak    | Servis     |
   |------|----------|-----------|------------|
   | 22   | TCP      | My IP     | SSH        |
   | 3000 | TCP      | 0.0.0.0/0 | Grafana    |
   | 5001 | TCP      | 0.0.0.0/0 | MLflow     |
   | 8080 | TCP      | 0.0.0.0/0 | Spark UI   |
   | 9092 | TCP      | 0.0.0.0/0 | Kafka      |

6. **Storage**: En az **30 GiB** (default 8 GiB yetmez)
7. **Launch Instance** → instance başlayana kadar bekle (~1 dk)

> Security Group kurallarını sonradan eklemek için:  
> EC2 → Instance → Security sekmesi → Security Group → Edit inbound rules

---

## 2. SSH ile Bağlanma

### Windows (PowerShell)
```powershell
ssh -i "C:\Users\AhmetCan\.ssh\bigdataproject.pem" ubuntu@<EC2_PUBLIC_IP>
```

İlk bağlantıda `Are you sure you want to continue connecting?` sorarsa **yes** yaz.

### Windows (PuTTY kullanıyorsan)
`.pem` dosyasını PuTTYgen ile `.ppk`'ya çevir, sonra PuTTY ile bağlan.

---

## 3. Pipeline Kurulumu

EC2'ye bağlandıktan sonra:

```bash
# Repo'yu clone et
git clone https://github.com/diclecoban/BigData-AirQuality.git
cd BigData-AirQuality

# Gerekli paketleri kur (Ubuntu 26.04 için)
sudo apt-get install -y python3-full
mkdir -p ~/pip-tmp

# Deploy script'i çalıştır
TMPDIR=$HOME/pip-tmp OPENAQ_API_KEY=<api_keyin> bash scripts/deploy_aws.sh
```

Script otomatik olarak şunları yapar:
- Docker kurar
- Kafka, Spark, MLflow, Grafana'yı başlatır
- Gerçek IBB + OpenAQ verisi çeker
- ML modellerini eğitir

Süre: ~20-30 dakika (ilk kurulumda image indirme dahil)

---

## 4. Servis Adresleri

Instance başladıktan sonra tarayıcıdan aç:

| Servis   | Adres                          | Giriş        |
|----------|--------------------------------|--------------|
| Grafana  | `http://<EC2_IP>:3000`         | admin / admin |
| MLflow   | `http://<EC2_IP>:5001`         | —            |
| Spark UI | `http://<EC2_IP>:8080`         | —            |
| Dashboard | `http://<EC2_IP>:8766/pipeline.html` | —       |

---

## 5. Servisleri Durdurma / Başlatma

```bash
# Durdur
cd ~/BigData-AirQuality/infra
docker compose -f docker-compose.yml -f docker-compose.aws.yml down

# Tekrar başlat
docker compose -f docker-compose.yml -f docker-compose.aws.yml up -d
```

Kod güncellemesinden sonra dashboard image'ını yeniden oluştur:

```bash
cd ~/BigData-AirQuality
git pull origin main
cd infra
docker compose -f docker-compose.yml -f docker-compose.aws.yml up -d --build dashboard
docker compose -f docker-compose.yml -f docker-compose.aws.yml logs --tail=100 dashboard
```

---

## 6. Maliyet Tahmini (eu-north-1 Stockholm)

| Kaynak      | Tip        | Saatlik  | Aylık (~)  |
|-------------|------------|----------|------------|
| EC2         | t3.xlarge  | ~$0.17   | ~$122      |
| EC2         | t3.large   | ~$0.08   | ~$60       |
| EBS Storage | 30 GiB gp3 | —        | ~$2.40     |

> **Ücret oluşmaması için:** Kullanmadığında instance'ı **durdur** (terminate değil).  
> AWS Console → EC2 → Instance → Instance state → **Stop**

---

## 7. Sık Karşılaşılan Sorunlar

| Sorun | Çözüm |
|-------|-------|
| `permission denied` (docker) | `newgrp docker` çalıştır |
| `Disk quota exceeded` (pip) | `TMPDIR=$HOME/pip-tmp` ile çalıştır |
| Kafka başlamıyor (OOM) | `infra/docker-compose.aws.yml`'de `KAFKA_HEAP_OPTS: "-Xmx512m -Xms256m"` var, kontrol et |
| Port'a erişilemiyor | Security Group inbound kurallarını kontrol et |
| SSH bağlantısı kopuyor | `tmux` veya `screen` kullan: `tmux new -s deploy` |
