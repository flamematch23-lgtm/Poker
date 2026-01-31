# Texas Hold'em Poker Server v12 - Multiplayer Completo

Server WebSocket per partite multiplayer online con:
- Sistema di partite private con tavoli reali
- Sistema livelli account basato su partite giocate
- Tavoli cash con centesimi (€0.10/€0.20, €0.25/€0.50, etc.)
- PayPal integration completa (depositi e prelievi)
- Reconnection system

## Novità v12:
1. **Partite Private REALI**: Ora creano tavoli effettivi con multiplayer funzionante
2. **Sistema Livelli**: Livello calcolato da games_played (1 livello ogni 10 partite)
3. **Blinds con Centesimi**: Supporto float per blinds (€0.10, €0.25, etc.)
4. **5 Tavoli Cash Predefiniti**:
   - Micro Stakes: €0.10/€0.20
   - Low Stakes: €0.25/€0.50
   - Medium Stakes: €0.50/€1.00
   - High Stakes: €1.00/€2.00
   - VIP Stakes: €2.50/€5.00

## Deploy
Railway, Render, or any Python hosting

## Environment Variables (optional)
- PAYPAL_CLIENT_ID
- PAYPAL_SECRET
- PORT (default: 8765)
