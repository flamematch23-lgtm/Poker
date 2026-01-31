# 🃏 Poker Texas Hold'em - Server Online

Server WebSocket per l'app Poker Texas Hold'em.

## Deploy su Railway.app (Consigliato - Gratis)

1. Vai su https://railway.app
2. Clicca "Start a New Project"
3. Seleziona "Deploy from GitHub repo" o carica questi file
4. Railway rileverà automaticamente il Procfile
5. Una volta deployato, copia l'URL (es. `poker-server.up.railway.app`)

## Deploy su Render.com (Alternativa)

1. Vai su https://render.com
2. Crea un nuovo "Web Service"
3. Collega il repository o carica i file
4. Imposta:
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `python server_online.py`
5. Copia l'URL del servizio

## URL WebSocket

Dopo il deploy, l'URL WebSocket sarà:
- Railway: `wss://tuo-progetto.up.railway.app`
- Render: `wss://tuo-progetto.onrender.com`

## File inclusi

- `server_online.py` - Server WebSocket principale
- `requirements.txt` - Dipendenze Python
- `Procfile` - Configurazione per Heroku/Railway
