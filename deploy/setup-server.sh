#!/bin/bash
# =========================================================
# FIAT — Setup del servidor (Ubuntu 22.04 / 24.04)
# Ejecutar como root o con sudo
# =========================================================

set -e

echo "=== FIAT - Configuración del servidor ==="

# 1. Actualizar sistema
apt update && apt upgrade -y

# 2. Instalar dependencias del sistema
apt install -y python3 python3-pip python3-venv nginx certbot python3-certbot-nginx git

# 3. Crear usuario fiat
useradd -m -s /bin/bash fiat || true

# 4. Clonar repositorio (como usuario fiat)
su - fiat -c '
    cd /home/fiat
    git clone https://github.com/TU-USUARIO/semaforo-legislativo.git || true
    cd semaforo-legislativo
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
'

# 5. Configurar Nginx
cp /home/fiat/semaforo-legislativo/deploy/nginx-fiat.conf /etc/nginx/sites-available/fiat
ln -sf /etc/nginx/sites-available/fiat /etc/nginx/sites-enabled/fiat
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl reload nginx

# 6. Configurar cron (cada 2 horas)
cat > /etc/cron.d/fiat-pipeline << 'CRON'
# FIAT - Pipeline cada 2 horas
0 */2 * * * fiat cd /home/fiat/semaforo-legislativo && /home/fiat/semaforo-legislativo/venv/bin/python main.py --skip-trends >> /home/fiat/semaforo-legislativo/logs/cron.log 2>&1

# Google Trends cada 6 horas (rate-limited)
0 */6 * * * fiat cd /home/fiat/semaforo-legislativo && /home/fiat/semaforo-legislativo/venv/bin/python main.py >> /home/fiat/semaforo-legislativo/logs/cron-full.log 2>&1
CRON

# 7. Crear directorio de logs
su - fiat -c 'mkdir -p /home/fiat/semaforo-legislativo/logs'

# 8. Primera ejecución del pipeline
echo "Ejecutando pipeline por primera vez..."
su - fiat -c '
    cd /home/fiat/semaforo-legislativo
    source venv/bin/activate
    python main.py --skip-trends
'

echo ""
echo "=== FIAT configurado exitosamente ==="
echo ""
echo "Próximos pasos:"
echo "  1. Edita /etc/nginx/sites-available/fiat y cambia 'tu-dominio.com' por tu dominio"
echo "  2. Apunta el DNS A record de tu dominio a la IP de este servidor"
echo "  3. Ejecuta: certbot --nginx -d tu-dominio.com"
echo "  4. Verifica: curl http://localhost"
echo ""
