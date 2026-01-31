"""
Texas Hold'em Poker - Server Online Avanzato
Creato per Gabriele

Server WebSocket per partite multiplayer online con:
- Sistema di stanze/tavoli
- Lobby con chat
- Matchmaking automatico
- Tornei
- Statistiche giocatori

Requisiti: pip install websockets aiosqlite
Esegui: python server_online.py
"""

import asyncio
import websockets
import json
import random
import string
import hashlib
import time
import sqlite3
import threading
import aiohttp
import base64
from datetime import datetime
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from enum import Enum
import os

# ============================================
# CONFIGURAZIONE SERVER
# ============================================

SERVER_HOST = "0.0.0.0"  # Accetta connessioni da qualsiasi IP
SERVER_PORT = int(os.environ.get("PORT", 8765))  # Usa PORT da ambiente per cloud hosting
MAX_PLAYERS_PER_TABLE = 9
MIN_PLAYERS_TO_START = 2
DATABASE_FILE = "poker_database.db"

# ============================================
# PAYPAL API CONFIGURATION
# ============================================
PAYPAL_CLIENT_ID = "AdVh2EpipQm930-jBn_EiP_2wxKjIaE5q5-trEjPa1c2q2HLNntj9PvseFSkvl9OVq_59_t8ICZzfLR9"
PAYPAL_SECRET = "EGXekVw-r7DhFjo4rCcm8U6x0Euh3h7iQpu67FxODbaAVL14-I0vjZBE-37a-9cIHJSMm0bhAETUc1oK"
# Use sandbox for testing, change to live for production
PAYPAL_MODE = "sandbox"  # "sandbox" or "live"
PAYPAL_API_BASE = "https://api-m.sandbox.paypal.com" if PAYPAL_MODE == "sandbox" else "https://api-m.paypal.com"

# ============================================
# ENUMS E COSTANTI
# ============================================

class GameState(Enum):
    WAITING = "waiting"
    PREFLOP = "preflop"
    FLOP = "flop"
    TURN = "turn"
    RIVER = "river"
    SHOWDOWN = "showdown"
    FINISHED = "finished"

class TournamentState(Enum):
    REGISTERING = "registering"
    RUNNING = "running"
    FINAL_TABLE = "final_table"
    FINISHED = "finished"

CARD_SUITS = ['hearts', 'diamonds', 'clubs', 'spades']
CARD_VALUES = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']

HAND_RANKINGS = {
    'high_card': 1,
    'pair': 2,
    'two_pair': 3,
    'three_of_a_kind': 4,
    'straight': 5,
    'flush': 6,
    'full_house': 7,
    'four_of_a_kind': 8,
    'straight_flush': 9,
    'royal_flush': 10
}

# ============================================
# PAYPAL API FUNCTIONS
# ============================================

class PayPalAPI:
    """Handles all PayPal API interactions"""
    
    def __init__(self):
        self.access_token = None
        self.token_expiry = 0
    
    async def get_access_token(self) -> str:
        """Get OAuth2 access token from PayPal"""
        if self.access_token and time.time() < self.token_expiry:
            return self.access_token
        
        auth_string = f"{PAYPAL_CLIENT_ID}:{PAYPAL_SECRET}"
        auth_bytes = base64.b64encode(auth_string.encode()).decode()
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{PAYPAL_API_BASE}/v1/oauth2/token",
                headers={
                    "Authorization": f"Basic {auth_bytes}",
                    "Content-Type": "application/x-www-form-urlencoded"
                },
                data="grant_type=client_credentials"
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.access_token = data["access_token"]
                    self.token_expiry = time.time() + data.get("expires_in", 3600) - 60
                    return self.access_token
                else:
                    error = await resp.text()
                    raise Exception(f"PayPal auth failed: {error}")
    
    async def create_order(self, amount: float, currency: str = "EUR", 
                          description: str = "Poker Chips Deposit") -> dict:
        """Create a PayPal order for deposit"""
        token = await self.get_access_token()
        
        order_data = {
            "intent": "CAPTURE",
            "purchase_units": [{
                "amount": {
                    "currency_code": currency,
                    "value": f"{amount:.2f}"
                },
                "description": description
            }],
            "application_context": {
                "brand_name": "PokerTexas",
                "landing_page": "NO_PREFERENCE",
                "user_action": "PAY_NOW",
                "return_url": "https://example.com/success",
                "cancel_url": "https://example.com/cancel"
            }
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{PAYPAL_API_BASE}/v2/checkout/orders",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json=order_data
            ) as resp:
                if resp.status in [200, 201]:
                    data = await resp.json()
                    # Find approval URL
                    approval_url = None
                    for link in data.get("links", []):
                        if link.get("rel") == "approve":
                            approval_url = link.get("href")
                            break
                    return {
                        "success": True,
                        "order_id": data["id"],
                        "status": data["status"],
                        "approval_url": approval_url
                    }
                else:
                    error = await resp.text()
                    return {"success": False, "error": f"Failed to create order: {error}"}
    
    async def capture_order(self, order_id: str) -> dict:
        """Capture (complete) a PayPal order after user approval"""
        token = await self.get_access_token()
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{PAYPAL_API_BASE}/v2/checkout/orders/{order_id}/capture",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }
            ) as resp:
                if resp.status in [200, 201]:
                    data = await resp.json()
                    # Extract capture details
                    captures = data.get("purchase_units", [{}])[0].get("payments", {}).get("captures", [])
                    if captures:
                        capture = captures[0]
                        return {
                            "success": True,
                            "capture_id": capture.get("id"),
                            "status": capture.get("status"),
                            "amount": float(capture.get("amount", {}).get("value", 0)),
                            "currency": capture.get("amount", {}).get("currency_code", "EUR")
                        }
                    return {"success": True, "status": data.get("status")}
                else:
                    error = await resp.text()
                    return {"success": False, "error": f"Failed to capture order: {error}"}
    
    async def get_order_details(self, order_id: str) -> dict:
        """Get details of a PayPal order"""
        token = await self.get_access_token()
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{PAYPAL_API_BASE}/v2/checkout/orders/{order_id}",
                headers={
                    "Authorization": f"Bearer {token}"
                }
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {"success": True, "order": data}
                else:
                    error = await resp.text()
                    return {"success": False, "error": error}
    
    async def send_payout(self, recipient_email: str, amount: float, 
                         currency: str = "EUR", note: str = "Poker winnings withdrawal") -> dict:
        """Send money to user via PayPal Payouts API"""
        token = await self.get_access_token()
        
        batch_id = f"PAYOUT_{int(time.time())}_{random.randint(1000, 9999)}"
        
        payout_data = {
            "sender_batch_header": {
                "sender_batch_id": batch_id,
                "email_subject": "PokerTexas - Prelievo",
                "email_message": "Hai ricevuto il tuo prelievo da PokerTexas!"
            },
            "items": [{
                "recipient_type": "EMAIL",
                "amount": {
                    "value": f"{amount:.2f}",
                    "currency": currency
                },
                "receiver": recipient_email,
                "note": note,
                "sender_item_id": f"ITEM_{batch_id}"
            }]
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{PAYPAL_API_BASE}/v1/payments/payouts",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json=payout_data
            ) as resp:
                if resp.status in [200, 201]:
                    data = await resp.json()
                    return {
                        "success": True,
                        "payout_batch_id": data.get("batch_header", {}).get("payout_batch_id"),
                        "batch_status": data.get("batch_header", {}).get("batch_status")
                    }
                else:
                    error = await resp.text()
                    return {"success": False, "error": f"Payout failed: {error}"}

# Global PayPal API instance
paypal_api = PayPalAPI()

# ============================================
# DATABASE MANAGER
# ============================================

class DatabaseManager:
    def __init__(self, db_file: str):
        self.db_file = db_file
        # Use a single persistent connection (avoid database locking issues)
        self.conn = sqlite3.connect(db_file, timeout=30.0, check_same_thread=False)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA busy_timeout=30000')
        self.init_database()
    
    def get_connection(self):
        """Return the persistent connection"""
        return self.conn
    
    def init_database(self):
        """Inizializza il database con le tabelle necessarie"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Tabella utenti
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                chips INTEGER DEFAULT 10000,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP
            )
        ''')
        
        # Tabella statistiche
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS statistics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                games_played INTEGER DEFAULT 0,
                games_won INTEGER DEFAULT 0,
                hands_played INTEGER DEFAULT 0,
                hands_won INTEGER DEFAULT 0,
                total_chips_won INTEGER DEFAULT 0,
                total_chips_lost INTEGER DEFAULT 0,
                biggest_pot_won INTEGER DEFAULT 0,
                best_hand TEXT DEFAULT '',
                royal_flushes INTEGER DEFAULT 0,
                straight_flushes INTEGER DEFAULT 0,
                four_of_kinds INTEGER DEFAULT 0,
                full_houses INTEGER DEFAULT 0,
                flushes INTEGER DEFAULT 0,
                straights INTEGER DEFAULT 0,
                tournaments_played INTEGER DEFAULT 0,
                tournaments_won INTEGER DEFAULT 0,
                tournament_cashes INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')
        
        # Tabella storico partite
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS game_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                game_type TEXT NOT NULL,
                result TEXT NOT NULL,
                chips_change INTEGER DEFAULT 0,
                played_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                details TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')
        
        # Tabella tornei
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournaments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                buy_in INTEGER NOT NULL,
                prize_pool INTEGER DEFAULT 0,
                max_players INTEGER DEFAULT 100,
                current_players INTEGER DEFAULT 0,
                state TEXT DEFAULT 'registering',
                winner_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                finished_at TIMESTAMP
            )
        ''')
        
        # Tabella risultati tornei
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                position INTEGER NOT NULL,
                prize INTEGER DEFAULT 0,
                FOREIGN KEY (tournament_id) REFERENCES tournaments(id),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')
        
        # Tabella wallet
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wallets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                balance REAL DEFAULT 0.00,
                in_game REAL DEFAULT 0.00,
                gold_tokens INTEGER DEFAULT 100,
                daily_limit REAL DEFAULT 500.00,
                today_deposits REAL DEFAULT 0.00,
                today_withdrawals REAL DEFAULT 0.00,
                today_winloss REAL DEFAULT 0.00,
                last_reset DATE DEFAULT CURRENT_DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')
        
        # Tabella transazioni
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                amount REAL NOT NULL,
                method TEXT,
                status TEXT DEFAULT 'completed',
                paypal_order_id TEXT,
                paypal_email TEXT,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')
        
        # Tabella partite private
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS private_games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT UNIQUE NOT NULL,
                type TEXT NOT NULL,
                name TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                creator_id INTEGER NOT NULL,
                settings TEXT,
                status TEXT DEFAULT 'waiting',
                start_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (creator_id) REFERENCES users(id)
            )
        ''')
        
        conn.commit()
        # conn.close() - Use persistent connection, don't close
    
    def create_user(self, username: str, password: str) -> Optional[int]:
        """Crea un nuovo utente"""
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                'INSERT INTO users (username, password_hash) VALUES (?, ?)',
                (username, password_hash)
            )
            user_id = cursor.lastrowid
            
            # Crea statistiche iniziali
            cursor.execute(
                'INSERT INTO statistics (user_id) VALUES (?)',
                (user_id,)
            )
            
            conn.commit()
            # conn.close() - Use persistent connection, don't close
            return user_id
        except sqlite3.IntegrityError:
            return None
    
    def authenticate_user(self, username: str, password: str) -> Optional[dict]:
        """Autentica un utente"""
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            'SELECT id, username, chips FROM users WHERE username = ? AND password_hash = ?',
            (username, password_hash)
        )
        
        row = cursor.fetchone()
        # conn.close() - Use persistent connection, don't close
        
        if row:
            return {'id': row[0], 'username': row[1], 'chips': row[2]}
        return None
    
    def get_user_stats(self, user_id: int) -> dict:
        """Ottiene le statistiche di un utente"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM statistics WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        
        if row:
            columns = [description[0] for description in cursor.description]
            stats = dict(zip(columns, row))
        else:
            stats = {}
        
        # conn.close() - Use persistent connection, don't close
        return stats
    
    def update_stats(self, user_id: int, updates: dict):
        """Aggiorna le statistiche di un utente"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        set_clause = ', '.join([f'{k} = {k} + ?' for k in updates.keys()])
        values = list(updates.values()) + [user_id]
        
        cursor.execute(
            f'UPDATE statistics SET {set_clause} WHERE user_id = ?',
            values
        )
        
        conn.commit()
        # conn.close() - Use persistent connection, don't close
    
    def set_stat(self, user_id: int, stat_name: str, value):
        """Imposta un valore specifico di statistica"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            f'UPDATE statistics SET {stat_name} = ? WHERE user_id = ?',
            (value, user_id)
        )
        
        conn.commit()
        # conn.close() - Use persistent connection, don't close
    
    def update_chips(self, user_id: int, amount: int):
        """Aggiorna le chips di un utente"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            'UPDATE users SET chips = chips + ? WHERE id = ?',
            (amount, user_id)
        )
        
        conn.commit()
        # conn.close() - Use persistent connection, don't close
    
    def get_leaderboard(self, limit: int = 10) -> List[dict]:
        """Ottiene la classifica dei migliori giocatori"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT u.username, u.chips, s.games_won, s.tournaments_won
            FROM users u
            JOIN statistics s ON u.id = s.user_id
            ORDER BY u.chips DESC
            LIMIT ?
        ''', (limit,))
        
        rows = cursor.fetchall()
        # conn.close() - Use persistent connection, don't close
        
        return [
            {
                'username': row[0],
                'chips': row[1],
                'games_won': row[2],
                'tournaments_won': row[3]
            }
            for row in rows
        ]
    
    def add_game_history(self, user_id: int, game_type: str, result: str, 
                         chips_change: int, details: str = ''):
        """Aggiunge una partita allo storico"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO game_history (user_id, game_type, result, chips_change, details)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, game_type, result, chips_change, details))
        
        conn.commit()
        # conn.close() - Use persistent connection, don't close
    
    def get_game_history(self, user_id: int, limit: int = 20) -> List[dict]:
        """Ottiene lo storico partite di un utente"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT game_type, result, chips_change, played_at, details
            FROM game_history
            WHERE user_id = ?
            ORDER BY played_at DESC
            LIMIT ?
        ''', (user_id, limit))
        
        rows = cursor.fetchall()
        # conn.close() - Use persistent connection, don't close
        
        return [
            {
                'game_type': row[0],
                'result': row[1],
                'chips_change': row[2],
                'played_at': row[3],
                'details': row[4]
            }
            for row in rows
        ]
    
    # ============================================
    # WALLET METHODS
    # ============================================
    
    def get_or_create_wallet(self, user_id: int) -> dict:
        """Ottiene o crea il wallet di un utente"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Check if daily reset is needed
        cursor.execute('SELECT * FROM wallets WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        
        if not row:
            # Create new wallet
            cursor.execute('''
                INSERT INTO wallets (user_id, balance, gold_tokens) 
                VALUES (?, 0.00, 100)
            ''', (user_id,))
            conn.commit()
            cursor.execute('SELECT * FROM wallets WHERE user_id = ?', (user_id,))
            row = cursor.fetchone()
        
        columns = [description[0] for description in cursor.description]
        wallet = dict(zip(columns, row))
        
        # Check if we need to reset daily limits
        today = datetime.now().date().isoformat()
        if wallet.get('last_reset') != today:
            cursor.execute('''
                UPDATE wallets 
                SET today_deposits = 0, today_withdrawals = 0, today_winloss = 0, last_reset = ?
                WHERE user_id = ?
            ''', (today, user_id))
            conn.commit()
            wallet['today_deposits'] = 0
            wallet['today_withdrawals'] = 0
            wallet['today_winloss'] = 0
        
        return wallet
    
    def update_wallet_balance(self, user_id: int, amount: float, 
                              transaction_type: str, details: dict = None) -> dict:
        """Aggiorna il saldo del wallet"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Get current wallet
        wallet = self.get_or_create_wallet(user_id)
        
        # Update balance
        new_balance = wallet['balance'] + amount
        if new_balance < 0:
            return {'success': False, 'error': 'Saldo insufficiente'}
        
        # Update wallet
        if transaction_type == 'deposit':
            cursor.execute('''
                UPDATE wallets 
                SET balance = balance + ?, today_deposits = today_deposits + ?
                WHERE user_id = ?
            ''', (amount, amount, user_id))
        elif transaction_type == 'withdrawal':
            cursor.execute('''
                UPDATE wallets 
                SET balance = balance - ?, today_withdrawals = today_withdrawals + ?
                WHERE user_id = ?
            ''', (abs(amount), abs(amount), user_id))
        elif transaction_type in ['game_win', 'game_loss']:
            cursor.execute('''
                UPDATE wallets 
                SET balance = balance + ?, today_winloss = today_winloss + ?
                WHERE user_id = ?
            ''', (amount, amount, user_id))
        else:
            cursor.execute('''
                UPDATE wallets SET balance = balance + ? WHERE user_id = ?
            ''', (amount, user_id))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, type, amount, method, status, details)
            VALUES (?, ?, ?, ?, 'completed', ?)
        ''', (user_id, transaction_type, amount, details.get('method') if details else 'system',
              json.dumps(details) if details else None))
        
        conn.commit()
        
        return {'success': True, 'new_balance': new_balance}
    
    def process_paypal_deposit(self, user_id: int, amount: float, 
                               paypal_order_id: str, paypal_details: dict) -> dict:
        """Processa un deposito PayPal"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Verify daily limit
        wallet = self.get_or_create_wallet(user_id)
        if wallet['today_deposits'] + amount > wallet['daily_limit']:
            return {'success': False, 'error': 'Limite giornaliero superato'}
        
        # Update balance
        cursor.execute('''
            UPDATE wallets 
            SET balance = balance + ?, today_deposits = today_deposits + ?
            WHERE user_id = ?
        ''', (amount, amount, user_id))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions 
            (user_id, type, amount, method, status, paypal_order_id, details)
            VALUES (?, 'deposit', ?, 'PayPal', 'completed', ?, ?)
        ''', (user_id, amount, paypal_order_id, json.dumps(paypal_details)))
        
        conn.commit()
        
        return {
            'success': True, 
            'new_balance': wallet['balance'] + amount,
            'transaction_id': cursor.lastrowid
        }
    
    def request_withdrawal(self, user_id: int, amount: float, paypal_email: str) -> dict:
        """Richiede un prelievo"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        wallet = self.get_or_create_wallet(user_id)
        
        if amount < 5:
            return {'success': False, 'error': 'Prelievo minimo €5'}
        
        if amount > wallet['balance']:
            return {'success': False, 'error': 'Saldo insufficiente'}
        
        # Deduct from balance
        cursor.execute('''
            UPDATE wallets 
            SET balance = balance - ?, today_withdrawals = today_withdrawals + ?
            WHERE user_id = ?
        ''', (amount, amount, user_id))
        
        # Record transaction (pending)
        cursor.execute('''
            INSERT INTO transactions 
            (user_id, type, amount, method, status, paypal_email, details)
            VALUES (?, 'withdrawal', ?, 'PayPal', 'pending', ?, ?)
        ''', (user_id, -amount, paypal_email, json.dumps({'paypal_email': paypal_email})))
        
        conn.commit()
        
        return {
            'success': True,
            'new_balance': wallet['balance'] - amount,
            'transaction_id': cursor.lastrowid
        }
    
    def get_transactions(self, user_id: int, limit: int = 50) -> list:
        """Ottiene lo storico transazioni"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, type, amount, method, status, paypal_order_id, created_at
            FROM transactions
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        ''', (user_id, limit))
        
        rows = cursor.fetchall()
        
        return [
            {
                'id': row[0],
                'type': row[1],
                'amount': row[2],
                'method': row[3],
                'status': row[4],
                'paypal_order_id': row[5],
                'timestamp': row[6]
            }
            for row in rows
        ]
    
    def add_gold_tokens(self, user_id: int, amount: int, reason: str = '') -> int:
        """Aggiunge gettoni d'oro"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        self.get_or_create_wallet(user_id)
        
        cursor.execute('''
            UPDATE wallets SET gold_tokens = gold_tokens + ? WHERE user_id = ?
        ''', (amount, user_id))
        
        conn.commit()
        
        cursor.execute('SELECT gold_tokens FROM wallets WHERE user_id = ?', (user_id,))
        return cursor.fetchone()[0]
    
    # ============================================
    # PRIVATE/FRIEND GAMES METHODS
    # ============================================
    
    def create_private_game(self, creator_id: int, name: str, password: str, 
                           game_type: str, settings: dict) -> dict:
        """Crea una partita privata tra amici"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        game_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        
        try:
            cursor.execute('''
                INSERT INTO private_games 
                (game_id, type, name, password_hash, creator_id, settings, status)
                VALUES (?, ?, ?, ?, ?, ?, 'waiting')
            ''', (game_id, game_type, name, password_hash, creator_id, json.dumps(settings)))
            
            conn.commit()
            
            return {
                'success': True,
                'game_id': game_id,
                'name': name,
                'type': game_type
            }
        except sqlite3.IntegrityError:
            return {'success': False, 'error': 'Nome partita già in uso'}
    
    def get_private_games(self, user_id: int = None) -> list:
        """Ottiene le partite private"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if user_id:
            cursor.execute('''
                SELECT pg.game_id, pg.type, pg.name, u.username, pg.settings, pg.status, pg.created_at
                FROM private_games pg
                JOIN users u ON pg.creator_id = u.id
                WHERE pg.creator_id = ? OR pg.status = 'waiting'
                ORDER BY pg.created_at DESC
                LIMIT 50
            ''', (user_id,))
        else:
            cursor.execute('''
                SELECT pg.game_id, pg.type, pg.name, u.username, pg.settings, pg.status, pg.created_at
                FROM private_games pg
                JOIN users u ON pg.creator_id = u.id
                WHERE pg.status = 'waiting'
                ORDER BY pg.created_at DESC
                LIMIT 50
            ''')
        
        rows = cursor.fetchall()
        games = []
        
        for row in rows:
            settings = json.loads(row[4]) if row[4] else {}
            games.append({
                'id': row[0],
                'game_type': row[1],
                'name': row[2],
                'creator': row[3],
                'buy_in': settings.get('buy_in', 0),
                'max_players': settings.get('max_players', 6),
                'small_blind': settings.get('small_blind', 1),
                'big_blind': settings.get('big_blind', 2),
                'current_players': 0,  # TODO: Track actual players
                'status': row[5],
                'created_at': row[6]
            })
        
        return games
    
    def join_private_game(self, user_id: int, game_name: str, password: str) -> dict:
        """Unisciti a una partita privata"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        cursor.execute('''
            SELECT game_id, type, name, settings, status
            FROM private_games
            WHERE name = ? AND password_hash = ? AND status = 'waiting'
        ''', (game_name, password_hash))
        
        row = cursor.fetchone()
        
        if not row:
            return {'success': False, 'error': 'Partita non trovata o password errata'}
        
        return {
            'success': True,
            'game_id': row[0],
            'type': row[1],
            'name': row[2],
            'settings': json.loads(row[3]) if row[3] else {}
        }


# ============================================
# CLASSI DEL GIOCO
# ============================================

@dataclass
class Card:
    suit: str
    value: str
    
    def to_dict(self):
        return {'suit': self.suit, 'value': self.value}
    
    def __str__(self):
        symbols = {'hearts': '♥', 'diamonds': '♦', 'clubs': '♣', 'spades': '♠'}
        return f"{self.value}{symbols[self.suit]}"


class Deck:
    def __init__(self):
        self.cards = []
        self.reset()
    
    def reset(self):
        self.cards = [Card(suit, value) for suit in CARD_SUITS for value in CARD_VALUES]
        random.shuffle(self.cards)
    
    def deal(self, count: int = 1) -> List[Card]:
        dealt = self.cards[:count]
        self.cards = self.cards[count:]
        return dealt


@dataclass
class Player:
    id: str
    username: str
    user_id: int
    chips: int
    cards: List[Card] = None
    current_bet: int = 0
    is_folded: bool = False
    is_all_in: bool = False
    is_connected: bool = True
    seat: int = -1
    
    def __post_init__(self):
        if self.cards is None:
            self.cards = []
    
    def to_dict(self, hide_cards: bool = True):
        return {
            'id': self.id,
            'username': self.username,
            'chips': self.chips,
            'cards': [] if hide_cards else [c.to_dict() for c in self.cards],
            'current_bet': self.current_bet,
            'is_folded': self.is_folded,
            'is_all_in': self.is_all_in,
            'is_connected': self.is_connected,
            'seat': self.seat,
            'has_cards': len(self.cards) > 0
        }


class PokerTable:
    def __init__(self, table_id: str, name: str, small_blind: int = 10, 
                 big_blind: int = 20, min_buy_in: int = 200, max_buy_in: int = 2000):
        self.table_id = table_id
        self.name = name
        self.small_blind = small_blind
        self.big_blind = big_blind
        self.min_buy_in = min_buy_in
        self.max_buy_in = max_buy_in
        
        self.players: Dict[str, Player] = {}
        self.spectators: Set[str] = set()
        self.deck = Deck()
        self.community_cards: List[Card] = []
        self.pot = 0
        self.side_pots: List[dict] = []
        self.current_bet = 0
        self.state = GameState.WAITING
        
        self.dealer_seat = 0
        self.current_player_seat = -1
        self.last_raiser_seat = -1
        self.min_raise = big_blind
        
        self.hand_number = 0
        self.action_timeout = 30  # secondi per agire
    
    def add_player(self, player: Player, seat: int = -1) -> bool:
        """Aggiunge un giocatore al tavolo"""
        if len(self.players) >= MAX_PLAYERS_PER_TABLE:
            return False
        
        if seat == -1:
            # Trova un posto libero
            occupied_seats = {p.seat for p in self.players.values()}
            for s in range(MAX_PLAYERS_PER_TABLE):
                if s not in occupied_seats:
                    seat = s
                    break
        
        player.seat = seat
        self.players[player.id] = player
        return True
    
    def remove_player(self, player_id: str):
        """Rimuove un giocatore dal tavolo"""
        if player_id in self.players:
            del self.players[player_id]
    
    def get_active_players(self) -> List[Player]:
        """Ritorna i giocatori ancora in gioco"""
        return [p for p in self.players.values() 
                if not p.is_folded and p.chips > 0]
    
    def get_players_in_hand(self) -> List[Player]:
        """Ritorna i giocatori ancora nella mano corrente"""
        return [p for p in self.players.values() 
                if not p.is_folded and len(p.cards) > 0]
    
    def start_hand(self):
        """Inizia una nuova mano"""
        if len(self.players) < MIN_PLAYERS_TO_START:
            return False
        
        self.hand_number += 1
        self.deck.reset()
        self.community_cards = []
        self.pot = 0
        self.side_pots = []
        self.current_bet = 0
        self.min_raise = self.big_blind
        
        # Reset giocatori
        for player in self.players.values():
            player.cards = []
            player.current_bet = 0
            player.is_folded = False
            player.is_all_in = False
        
        # Muovi il dealer
        seats = sorted([p.seat for p in self.players.values()])
        current_dealer_idx = seats.index(self.dealer_seat) if self.dealer_seat in seats else -1
        self.dealer_seat = seats[(current_dealer_idx + 1) % len(seats)]
        
        # Posta blinds
        self._post_blinds()
        
        # Distribuisci carte
        for player in self.players.values():
            player.cards = self.deck.deal(2)
        
        self.state = GameState.PREFLOP
        self._set_next_player()
        
        return True
    
    def _post_blinds(self):
        """Posta small e big blind"""
        seats = sorted([p.seat for p in self.players.values()])
        dealer_idx = seats.index(self.dealer_seat)
        
        if len(seats) == 2:
            # Heads-up: dealer è small blind
            sb_seat = self.dealer_seat
            bb_seat = seats[(dealer_idx + 1) % 2]
        else:
            sb_seat = seats[(dealer_idx + 1) % len(seats)]
            bb_seat = seats[(dealer_idx + 2) % len(seats)]
        
        for player in self.players.values():
            if player.seat == sb_seat:
                self._place_bet(player, self.small_blind)
            elif player.seat == bb_seat:
                self._place_bet(player, self.big_blind)
                self.last_raiser_seat = player.seat
        
        self.current_bet = self.big_blind
    
    def _place_bet(self, player: Player, amount: int) -> int:
        """Piazza una puntata"""
        actual_bet = min(amount, player.chips)
        player.chips -= actual_bet
        player.current_bet += actual_bet
        self.pot += actual_bet
        
        if player.chips == 0:
            player.is_all_in = True
        
        return actual_bet
    
    def _set_next_player(self):
        """Imposta il prossimo giocatore a giocare"""
        active_players = [p for p in self.players.values() 
                        if not p.is_folded and not p.is_all_in]
        
        if len(active_players) <= 1:
            self._end_betting_round()
            return
        
        seats = sorted([p.seat for p in active_players])
        
        if self.current_player_seat == -1:
            # Inizio round
            if self.state == GameState.PREFLOP:
                # Dopo il big blind
                all_seats = sorted([p.seat for p in self.players.values()])
                dealer_idx = all_seats.index(self.dealer_seat)
                start_seat = all_seats[(dealer_idx + 3) % len(all_seats)]
            else:
                # Primo dopo il dealer
                start_seat = seats[0]
                for s in seats:
                    if s > self.dealer_seat:
                        start_seat = s
                        break
            
            self.current_player_seat = start_seat
        else:
            # Prossimo giocatore
            current_idx = seats.index(self.current_player_seat) if self.current_player_seat in seats else 0
            self.current_player_seat = seats[(current_idx + 1) % len(seats)]
        
        # Verifica se il round è finito
        if self.current_player_seat == self.last_raiser_seat:
            all_matched = all(
                p.current_bet == self.current_bet or p.is_all_in or p.is_folded
                for p in self.players.values()
            )
            if all_matched:
                self._end_betting_round()
    
    def process_action(self, player_id: str, action: str, amount: int = 0) -> dict:
        """Processa un'azione del giocatore"""
        player = self.players.get(player_id)
        if not player:
            return {'success': False, 'error': 'Giocatore non trovato'}
        
        if player.seat != self.current_player_seat:
            return {'success': False, 'error': 'Non è il tuo turno'}
        
        if player.is_folded or player.is_all_in:
            return {'success': False, 'error': 'Non puoi agire'}
        
        result = {'success': True, 'action': action, 'amount': 0}
        
        if action == 'fold':
            player.is_folded = True
            result['message'] = f'{player.username} passa'
            
        elif action == 'check':
            if player.current_bet < self.current_bet:
                return {'success': False, 'error': 'Non puoi checkare, devi chiamare o foldare'}
            result['message'] = f'{player.username} checka'
            
        elif action == 'call':
            call_amount = self.current_bet - player.current_bet
            if call_amount <= 0:
                return {'success': False, 'error': 'Niente da chiamare'}
            actual = self._place_bet(player, call_amount)
            result['amount'] = actual
            result['message'] = f'{player.username} chiama {actual}'
            
        elif action == 'bet':
            if self.current_bet > 0:
                return {'success': False, 'error': 'Usa raise per rilanciare'}
            if amount < self.big_blind:
                return {'success': False, 'error': f'Puntata minima: {self.big_blind}'}
            if amount > player.chips:
                amount = player.chips
            actual = self._place_bet(player, amount)
            self.current_bet = player.current_bet
            self.last_raiser_seat = player.seat
            self.min_raise = amount
            result['amount'] = actual
            result['message'] = f'{player.username} punta {actual}'
            
        elif action == 'raise':
            to_call = self.current_bet - player.current_bet
            min_raise_amount = self.current_bet + self.min_raise
            
            if amount < min_raise_amount and amount < player.chips:
                return {'success': False, 'error': f'Rilancio minimo: {min_raise_amount}'}
            
            if amount > player.chips:
                amount = player.chips
            
            actual = self._place_bet(player, amount)
            self.min_raise = player.current_bet - self.current_bet
            self.current_bet = player.current_bet
            self.last_raiser_seat = player.seat
            result['amount'] = actual
            result['message'] = f'{player.username} rilancia a {player.current_bet}'
            
        elif action == 'all_in':
            actual = self._place_bet(player, player.chips)
            if player.current_bet > self.current_bet:
                self.min_raise = player.current_bet - self.current_bet
                self.current_bet = player.current_bet
                self.last_raiser_seat = player.seat
            result['amount'] = actual
            result['message'] = f'{player.username} va all-in con {actual}'
        
        else:
            return {'success': False, 'error': 'Azione non valida'}
        
        # Controlla se rimane un solo giocatore
        active = [p for p in self.players.values() if not p.is_folded]
        if len(active) == 1:
            self._award_pot(active)
            self.state = GameState.FINISHED
            return result
        
        self._set_next_player()
        return result
    
    def _end_betting_round(self):
        """Termina il round di puntate corrente"""
        # Reset puntate
        for player in self.players.values():
            player.current_bet = 0
        self.current_bet = 0
        self.current_player_seat = -1
        self.last_raiser_seat = -1
        
        active = [p for p in self.players.values() if not p.is_folded]
        all_in_count = sum(1 for p in active if p.is_all_in)
        
        if len(active) == 1:
            self._award_pot(active)
            self.state = GameState.FINISHED
            return
        
        # Se tutti tranne uno sono all-in, vai direttamente allo showdown
        if all_in_count >= len(active) - 1:
            while self.state not in [GameState.SHOWDOWN, GameState.FINISHED]:
                self._advance_state()
            return
        
        self._advance_state()
    
    def _advance_state(self):
        """Avanza alla prossima fase del gioco"""
        if self.state == GameState.PREFLOP:
            self.community_cards = self.deck.deal(3)
            self.state = GameState.FLOP
        elif self.state == GameState.FLOP:
            self.community_cards.extend(self.deck.deal(1))
            self.state = GameState.TURN
        elif self.state == GameState.TURN:
            self.community_cards.extend(self.deck.deal(1))
            self.state = GameState.RIVER
        elif self.state == GameState.RIVER:
            self.state = GameState.SHOWDOWN
            self._showdown()
            return
        
        self._set_next_player()
    
    def _showdown(self):
        """Determina il vincitore e assegna il pot"""
        active = [p for p in self.players.values() if not p.is_folded]
        
        if len(active) == 1:
            self._award_pot(active)
        else:
            # Valuta le mani
            hands = []
            for player in active:
                all_cards = player.cards + self.community_cards
                hand_result = self._evaluate_hand(all_cards)
                hands.append((player, hand_result))
            
            # Ordina per forza della mano
            hands.sort(key=lambda x: (x[1]['rank'], x[1]['values']), reverse=True)
            
            # Trova i vincitori (potrebbero essere più di uno in caso di parità)
            best_hand = hands[0][1]
            winners = [h[0] for h in hands 
                      if h[1]['rank'] == best_hand['rank'] and h[1]['values'] == best_hand['values']]
            
            self._award_pot(winners)
            
            # Aggiorna mano migliore per le statistiche
            for player, hand in hands:
                hand['player_id'] = player.user_id
        
        self.state = GameState.FINISHED
    
    def _award_pot(self, winners: List[Player]):
        """Assegna il pot ai vincitori"""
        if not winners:
            return
        
        share = self.pot // len(winners)
        remainder = self.pot % len(winners)
        
        for i, winner in enumerate(winners):
            award = share + (1 if i < remainder else 0)
            winner.chips += award
    
    def _evaluate_hand(self, cards: List[Card]) -> dict:
        """Valuta una mano di poker"""
        # Converti le carte in formato numerico
        value_map = {v: i for i, v in enumerate(CARD_VALUES)}
        
        values = sorted([value_map[c.value] for c in cards], reverse=True)
        suits = [c.suit for c in cards]
        
        # Conta valori e semi
        value_counts = {}
        for v in values:
            value_counts[v] = value_counts.get(v, 0) + 1
        
        suit_counts = {}
        for s in suits:
            suit_counts[s] = suit_counts.get(s, 0) + 1
        
        # Controlla flush
        flush_suit = None
        for suit, count in suit_counts.items():
            if count >= 5:
                flush_suit = suit
                break
        
        flush_cards = [c for c in cards if c.suit == flush_suit] if flush_suit else []
        
        # Controlla scala
        unique_values = sorted(set(values), reverse=True)
        
        # Aggiungi Asso come 1 per scala bassa
        if 12 in unique_values:  # Asso
            unique_values.append(-1)
        
        straight_high = None
        for i in range(len(unique_values) - 4):
            if unique_values[i] - unique_values[i + 4] == 4:
                straight_high = unique_values[i]
                break
        
        # Controlla scala colore
        straight_flush_high = None
        if flush_suit:
            flush_values = sorted([value_map[c.value] for c in flush_cards], reverse=True)
            if 12 in flush_values:
                flush_values.append(-1)
            
            for i in range(len(flush_values) - 4):
                if flush_values[i] - flush_values[i + 4] == 4:
                    straight_flush_high = flush_values[i]
                    break
        
        # Determina il tipo di mano
        counts = sorted(value_counts.values(), reverse=True)
        
        if straight_flush_high is not None:
            if straight_flush_high == 12:  # Scala reale
                return {'rank': 10, 'name': 'royal_flush', 'values': [12]}
            return {'rank': 9, 'name': 'straight_flush', 'values': [straight_flush_high]}
        
        if counts[0] == 4:
            quad_val = [v for v, c in value_counts.items() if c == 4][0]
            kicker = max(v for v in values if v != quad_val)
            return {'rank': 8, 'name': 'four_of_a_kind', 'values': [quad_val, kicker]}
        
        if counts[0] == 3 and counts[1] >= 2:
            trip_val = [v for v, c in value_counts.items() if c == 3][0]
            pair_val = max(v for v, c in value_counts.items() if c >= 2 and v != trip_val)
            return {'rank': 7, 'name': 'full_house', 'values': [trip_val, pair_val]}
        
        if flush_suit:
            flush_vals = sorted([value_map[c.value] for c in flush_cards], reverse=True)[:5]
            return {'rank': 6, 'name': 'flush', 'values': flush_vals}
        
        if straight_high is not None:
            return {'rank': 5, 'name': 'straight', 'values': [straight_high]}
        
        if counts[0] == 3:
            trip_val = [v for v, c in value_counts.items() if c == 3][0]
            kickers = sorted([v for v in values if v != trip_val], reverse=True)[:2]
            return {'rank': 4, 'name': 'three_of_a_kind', 'values': [trip_val] + kickers}
        
        if counts[0] == 2 and counts[1] == 2:
            pairs = sorted([v for v, c in value_counts.items() if c == 2], reverse=True)[:2]
            kicker = max(v for v in values if v not in pairs)
            return {'rank': 3, 'name': 'two_pair', 'values': pairs + [kicker]}
        
        if counts[0] == 2:
            pair_val = [v for v, c in value_counts.items() if c == 2][0]
            kickers = sorted([v for v in values if v != pair_val], reverse=True)[:3]
            return {'rank': 2, 'name': 'pair', 'values': [pair_val] + kickers}
        
        return {'rank': 1, 'name': 'high_card', 'values': values[:5]}
    
    def to_dict(self, for_player_id: str = None) -> dict:
        """Converte lo stato del tavolo in dizionario"""
        players_data = {}
        for pid, player in self.players.items():
            # Mostra le carte solo al giocatore stesso o durante lo showdown
            hide_cards = pid != for_player_id and self.state != GameState.SHOWDOWN
            players_data[pid] = player.to_dict(hide_cards=hide_cards)
        
        return {
            'table_id': self.table_id,
            'name': self.name,
            'small_blind': self.small_blind,
            'big_blind': self.big_blind,
            'min_buy_in': self.min_buy_in,
            'max_buy_in': self.max_buy_in,
            'players': players_data,
            'player_count': len(self.players),
            'spectator_count': len(self.spectators),
            'community_cards': [c.to_dict() for c in self.community_cards],
            'pot': self.pot,
            'current_bet': self.current_bet,
            'state': self.state.value,
            'dealer_seat': self.dealer_seat,
            'current_player_seat': self.current_player_seat,
            'hand_number': self.hand_number,
            'min_raise': self.min_raise
        }


# ============================================
# TOURNAMENT MANAGER
# ============================================

class Tournament:
    def __init__(self, tournament_id: str, name: str, buy_in: int, 
                 starting_chips: int = 5000, max_players: int = 100):
        self.tournament_id = tournament_id
        self.name = name
        self.buy_in = buy_in
        self.starting_chips = starting_chips
        self.max_players = max_players
        
        self.players: Dict[str, dict] = {}  # player_id -> {user_id, username, chips, table_id, eliminated_position}
        self.tables: Dict[str, PokerTable] = {}
        self.state = TournamentState.REGISTERING
        self.prize_pool = 0
        self.prize_structure = {}
        
        self.blind_level = 1
        self.blind_increase_interval = 600  # secondi (10 minuti)
        self.last_blind_increase = 0
        
        self.eliminated_players: List[str] = []
    
    def register_player(self, player_id: str, user_id: int, username: str) -> bool:
        """Registra un giocatore al torneo"""
        if self.state != TournamentState.REGISTERING:
            return False
        if len(self.players) >= self.max_players:
            return False
        if player_id in self.players:
            return False
        
        self.players[player_id] = {
            'user_id': user_id,
            'username': username,
            'chips': self.starting_chips,
            'table_id': None,
            'eliminated_position': None
        }
        self.prize_pool += self.buy_in
        return True
    
    def unregister_player(self, player_id: str) -> bool:
        """Rimuove un giocatore dal torneo (prima dell'inizio)"""
        if self.state != TournamentState.REGISTERING:
            return False
        if player_id not in self.players:
            return False
        
        del self.players[player_id]
        self.prize_pool -= self.buy_in
        return True
    
    def start_tournament(self) -> bool:
        """Inizia il torneo"""
        if self.state != TournamentState.REGISTERING:
            return False
        if len(self.players) < 2:
            return False
        
        self.state = TournamentState.RUNNING
        self._calculate_prizes()
        self._create_tables()
        self.last_blind_increase = time.time()
        
        return True
    
    def _calculate_prizes(self):
        """Calcola la struttura dei premi"""
        num_players = len(self.players)
        
        if num_players <= 10:
            # Pagano top 3
            self.prize_structure = {
                1: int(self.prize_pool * 0.50),
                2: int(self.prize_pool * 0.30),
                3: int(self.prize_pool * 0.20)
            }
        elif num_players <= 30:
            # Pagano top 5
            self.prize_structure = {
                1: int(self.prize_pool * 0.40),
                2: int(self.prize_pool * 0.25),
                3: int(self.prize_pool * 0.15),
                4: int(self.prize_pool * 0.12),
                5: int(self.prize_pool * 0.08)
            }
        else:
            # Pagano top 10%
            paid_positions = max(5, num_players // 10)
            # Distribuzione esponenziale decrescente
            total_shares = sum(2 ** (paid_positions - i) for i in range(1, paid_positions + 1))
            
            self.prize_structure = {}
            for i in range(1, paid_positions + 1):
                share = 2 ** (paid_positions - i) / total_shares
                self.prize_structure[i] = int(self.prize_pool * share)
    
    def _create_tables(self):
        """Crea i tavoli del torneo"""
        player_ids = list(self.players.keys())
        random.shuffle(player_ids)
        
        num_tables = (len(player_ids) + MAX_PLAYERS_PER_TABLE - 1) // MAX_PLAYERS_PER_TABLE
        
        for i in range(num_tables):
            table_id = f"{self.tournament_id}_table_{i + 1}"
            blinds = self._get_current_blinds()
            
            table = PokerTable(
                table_id=table_id,
                name=f"Tavolo {i + 1}",
                small_blind=blinds[0],
                big_blind=blinds[1],
                min_buy_in=0,
                max_buy_in=0
            )
            
            self.tables[table_id] = table
        
        # Distribuisci giocatori ai tavoli
        table_ids = list(self.tables.keys())
        for i, player_id in enumerate(player_ids):
            table_id = table_ids[i % num_tables]
            self.players[player_id]['table_id'] = table_id
    
    def _get_current_blinds(self) -> tuple:
        """Ottiene i blinds correnti basati sul livello"""
        blind_levels = [
            (10, 20), (15, 30), (25, 50), (50, 100),
            (75, 150), (100, 200), (150, 300), (200, 400),
            (300, 600), (400, 800), (500, 1000), (600, 1200),
            (800, 1600), (1000, 2000), (1500, 3000), (2000, 4000)
        ]
        
        level_idx = min(self.blind_level - 1, len(blind_levels) - 1)
        return blind_levels[level_idx]
    
    def check_blind_increase(self):
        """Controlla se aumentare i blinds"""
        if time.time() - self.last_blind_increase >= self.blind_increase_interval:
            self.blind_level += 1
            self.last_blind_increase = time.time()
            
            blinds = self._get_current_blinds()
            for table in self.tables.values():
                table.small_blind = blinds[0]
                table.big_blind = blinds[1]
            
            return True
        return False
    
    def eliminate_player(self, player_id: str):
        """Elimina un giocatore dal torneo"""
        if player_id not in self.players:
            return
        
        remaining = len([p for p in self.players.values() if p['eliminated_position'] is None])
        self.players[player_id]['eliminated_position'] = remaining
        self.eliminated_players.append(player_id)
        
        # Rimuovi dal tavolo
        table_id = self.players[player_id]['table_id']
        if table_id and table_id in self.tables:
            self.tables[table_id].remove_player(player_id)
        
        # Controlla se il torneo è finito
        remaining_after = len([p for p in self.players.values() if p['eliminated_position'] is None])
        
        if remaining_after == 1:
            self._finish_tournament()
        elif remaining_after <= MAX_PLAYERS_PER_TABLE:
            self.state = TournamentState.FINAL_TABLE
            self._consolidate_to_final_table()
    
    def _consolidate_to_final_table(self):
        """Consolida i giocatori rimanenti in un tavolo finale"""
        remaining_players = [pid for pid, p in self.players.items() 
                           if p['eliminated_position'] is None]
        
        # Crea tavolo finale
        final_table_id = f"{self.tournament_id}_final"
        blinds = self._get_current_blinds()
        
        final_table = PokerTable(
            table_id=final_table_id,
            name="Tavolo Finale",
            small_blind=blinds[0],
            big_blind=blinds[1]
        )
        
        # Muovi tutti i giocatori al tavolo finale
        for player_id in remaining_players:
            self.players[player_id]['table_id'] = final_table_id
        
        # Rimuovi vecchi tavoli
        self.tables = {final_table_id: final_table}
    
    def _finish_tournament(self):
        """Termina il torneo"""
        self.state = TournamentState.FINISHED
        
        # Trova il vincitore
        winner_id = None
        for pid, p in self.players.items():
            if p['eliminated_position'] is None:
                winner_id = pid
                self.players[pid]['eliminated_position'] = 1
                break
    
    def get_standings(self) -> List[dict]:
        """Ottiene la classifica del torneo"""
        standings = []
        
        for pid, p in self.players.items():
            position = p['eliminated_position']
            prize = self.prize_structure.get(position, 0) if position else None
            
            standings.append({
                'player_id': pid,
                'username': p['username'],
                'chips': p['chips'],
                'position': position,
                'prize': prize,
                'eliminated': position is not None
            })
        
        # Ordina: prima chi è ancora in gioco (per chips), poi gli eliminati (per posizione)
        standings.sort(key=lambda x: (
            x['position'] is not None,  # False viene prima
            x['position'] if x['position'] else 0,
            -x['chips']
        ))
        
        return standings
    
    def to_dict(self) -> dict:
        return {
            'tournament_id': self.tournament_id,
            'name': self.name,
            'buy_in': self.buy_in,
            'starting_chips': self.starting_chips,
            'max_players': self.max_players,
            'current_players': len(self.players),
            'state': self.state.value,
            'prize_pool': self.prize_pool,
            'prize_structure': self.prize_structure,
            'blind_level': self.blind_level,
            'current_blinds': self._get_current_blinds(),
            'tables_count': len(self.tables),
            'players_remaining': len([p for p in self.players.values() if p['eliminated_position'] is None])
        }


# ============================================
# SERVER PRINCIPALE
# ============================================

class PokerServer:
    def __init__(self):
        self.db = DatabaseManager(DATABASE_FILE)
        self.clients: Dict[str, websockets.WebSocketServerProtocol] = {}
        self.players: Dict[str, dict] = {}  # connection_id -> {user_id, username, chips}
        self.tables: Dict[str, PokerTable] = {}
        self.tournaments: Dict[str, Tournament] = {}
        self.lobby_users: Set[str] = set()
        
        # Sistema reconnect: traccia tavolo attivo per ogni user_id
        self.user_active_tables: Dict[int, dict] = {}  # user_id -> {table_id, stack, seat}
        
        # Crea tavoli predefiniti
        self._create_default_tables()
    
    def _create_default_tables(self):
        """Crea i tavoli predefiniti"""
        default_tables = [
            ("table_low", "Principianti", 10, 20, 200, 2000),
            ("table_mid", "Intermedio", 25, 50, 500, 5000),
            ("table_high", "Esperti", 50, 100, 1000, 10000),
            ("table_vip", "VIP", 100, 200, 2000, 20000),
        ]
        
        for tid, name, sb, bb, min_b, max_b in default_tables:
            self.tables[tid] = PokerTable(tid, name, sb, bb, min_b, max_b)
    
    def _generate_id(self, prefix: str = "") -> str:
        """Genera un ID univoco"""
        return prefix + ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    
    async def register(self, websocket: websockets.WebSocketServerProtocol) -> str:
        """Registra una nuova connessione"""
        conn_id = self._generate_id("conn_")
        self.clients[conn_id] = websocket
        return conn_id
    
    async def unregister(self, conn_id: str):
        """Rimuove una connessione - ma salva stato per reconnect"""
        if conn_id in self.clients:
            del self.clients[conn_id]
        
        if conn_id in self.players:
            player_info = self.players[conn_id]
            user_id = player_info.get('user_id')
            
            # Salva stato tavolo per possibile reconnect
            for table_id, table in self.tables.items():
                if conn_id in table.players:
                    player = table.players[conn_id]
                    # Segna come disconnesso invece di rimuovere
                    player.is_connected = False
                    # Salva info per reconnect (conserva per 5 minuti)
                    self.user_active_tables[user_id] = {
                        'table_id': table_id,
                        'stack': player.chips,
                        'seat': player.seat,
                        'disconnected_at': time.time()
                    }
                    print(f"[RECONNECT] User {user_id} disconnesso dal tavolo {table_id}, stack salvato: {player.chips}")
            
            del self.players[conn_id]
        
        self.lobby_users.discard(conn_id)
    
    async def send_to(self, conn_id: str, message: dict):
        """Invia un messaggio a un client specifico"""
        if conn_id in self.clients:
            try:
                await self.clients[conn_id].send(json.dumps(message))
            except:
                pass
    
    async def broadcast_to_table(self, table_id: str, message: dict, exclude: str = None):
        """Invia un messaggio a tutti i giocatori di un tavolo"""
        if table_id not in self.tables:
            return
        
        table = self.tables[table_id]
        for player_id in list(table.players.keys()) + list(table.spectators):
            if player_id != exclude:
                await self.send_to(player_id, message)
    
    async def broadcast_lobby(self, message: dict):
        """Invia un messaggio a tutti nella lobby"""
        for user_id in self.lobby_users:
            await self.send_to(user_id, message)
    
    async def handle_message(self, conn_id: str, message: dict):
        """Gestisce un messaggio dal client"""
        action = message.get('action')
        
        handlers = {
            'register': self.handle_register,
            'login': self.handle_login,
            'get_tables': self.handle_get_tables,
            'create_table': self.handle_create_table,
            'join_table': self.handle_join_table,
            'leave_table': self.handle_leave_table,
            'game_action': self.handle_game_action,
            'chat': self.handle_chat,
            'get_stats': self.handle_get_stats,
            'get_leaderboard': self.handle_get_leaderboard,
            'get_history': self.handle_get_history,
            'get_tournaments': self.handle_get_tournaments,
            'create_tournament': self.handle_create_tournament,
            'join_tournament': self.handle_join_tournament,
            'leave_tournament': self.handle_leave_tournament,
            'start_game': self.handle_start_game,
            # Wallet handlers
            'get_wallet': self.handle_get_wallet,
            'wallet_deposit': self.handle_wallet_deposit,
            'capture_deposit': self.handle_capture_deposit,
            'wallet_withdraw': self.handle_wallet_withdraw,
            'get_transactions': self.handle_get_transactions,
            # Private games handlers
            'create_private_tournament': self.handle_create_private_tournament,
            'create_private_cashgame': self.handle_create_private_cashgame,
            'join_private_game': self.handle_join_private_game,
            # Friend games handlers (new)
            'get_friend_games': self.handle_get_friend_games,
            'create_friend_game': self.handle_create_friend_game,
            'join_friend_game': self.handle_join_friend_game,
            # Reconnect handler
            'check_active_table': self.handle_check_active_table,
            'reconnect_table': self.handle_reconnect_table,
        }
        
        handler = handlers.get(action)
        if handler:
            await handler(conn_id, message)
        else:
            await self.send_to(conn_id, {'type': 'error', 'message': 'Azione non riconosciuta'})
    
    async def handle_register(self, conn_id: str, message: dict):
        """Gestisce la registrazione di un nuovo utente"""
        username = message.get('username', '').strip()
        password = message.get('password', '')
        
        if not username or not password:
            await self.send_to(conn_id, {
                'type': 'register_result',
                'success': False,
                'error': 'Username e password richiesti'
            })
            return
        
        if len(username) < 3 or len(username) > 20:
            await self.send_to(conn_id, {
                'type': 'register_result',
                'success': False,
                'error': 'Username deve essere tra 3 e 20 caratteri'
            })
            return
        
        user_id = self.db.create_user(username, password)
        
        if user_id:
            self.players[conn_id] = {
                'user_id': user_id,
                'username': username,
                'chips': 10000
            }
            self.lobby_users.add(conn_id)
            
            await self.send_to(conn_id, {
                'type': 'register_result',
                'success': True,
                'user': self.players[conn_id]
            })
        else:
            await self.send_to(conn_id, {
                'type': 'register_result',
                'success': False,
                'error': 'Username già in uso'
            })
    
    async def handle_login(self, conn_id: str, message: dict):
        """Gestisce il login"""
        username = message.get('username', '')
        password = message.get('password', '')
        
        user = self.db.authenticate_user(username, password)
        
        if user:
            self.players[conn_id] = {
                'user_id': user['id'],
                'username': user['username'],
                'chips': user['chips']
            }
            self.lobby_users.add(conn_id)
            
            await self.send_to(conn_id, {
                'type': 'login_result',
                'success': True,
                'user': self.players[conn_id]
            })
        else:
            await self.send_to(conn_id, {
                'type': 'login_result',
                'success': False,
                'error': 'Credenziali non valide'
            })
    
    async def handle_get_tables(self, conn_id: str, message: dict):
        """Ritorna la lista dei tavoli"""
        tables_info = []
        
        for table in self.tables.values():
            tables_info.append({
                'table_id': table.table_id,
                'name': table.name,
                'small_blind': table.small_blind,
                'big_blind': table.big_blind,
                'min_buy_in': table.min_buy_in,
                'max_buy_in': table.max_buy_in,
                'player_count': len(table.players),
                'max_players': MAX_PLAYERS_PER_TABLE,
                'state': table.state.value
            })
        
        await self.send_to(conn_id, {
            'type': 'tables_list',
            'tables': tables_info,
            'online_count': len(self.clients)
        })
    
    async def handle_create_table(self, conn_id: str, message: dict):
        """Crea un nuovo tavolo"""
        name = message.get('name', 'Nuovo Tavolo')
        blinds = message.get('blinds', '25/50')
        starting_chips = message.get('startingChips', 5000)
        max_players = message.get('maxPlayers', 6)
        creator = message.get('creator', 'Anonimo')
        
        # Parse blinds
        try:
            sb, bb = blinds.split('/')
            small_blind = int(sb)
            big_blind = int(bb)
        except:
            small_blind = 25
            big_blind = 50
        
        # Generate unique table ID
        table_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        
        # Create the table
        table = PokerTable(
            table_id=table_id,
            name=f"{name} ({creator})",
            small_blind=small_blind,
            big_blind=big_blind,
            min_buy_in=starting_chips,
            max_buy_in=starting_chips
        )
        
        self.tables[table_id] = table
        
        await self.send_to(conn_id, {
            'type': 'table_created',
            'table_id': table_id,
            'message': f'Tavolo "{name}" creato! Codice: {table_id}'
        })
        
        # Broadcast to all connections
        await self.broadcast({
            'type': 'table_update',
            'action': 'created',
            'table_id': table_id
        })
    
    async def handle_join_table(self, conn_id: str, message: dict):
        """Gestisce l'entrata a un tavolo"""
        table_id = message.get('table_id')
        buy_in = message.get('buy_in', 0)
        
        if conn_id not in self.players:
            await self.send_to(conn_id, {
                'type': 'join_result',
                'success': False,
                'error': 'Devi effettuare il login'
            })
            return
        
        if table_id not in self.tables:
            await self.send_to(conn_id, {
                'type': 'join_result',
                'success': False,
                'error': 'Tavolo non trovato'
            })
            return
        
        table = self.tables[table_id]
        player_info = self.players[conn_id]
        
        # Verifica buy-in
        if buy_in < table.min_buy_in or buy_in > table.max_buy_in:
            await self.send_to(conn_id, {
                'type': 'join_result',
                'success': False,
                'error': f'Buy-in deve essere tra {table.min_buy_in} e {table.max_buy_in}'
            })
            return
        
        if buy_in > player_info['chips']:
            await self.send_to(conn_id, {
                'type': 'join_result',
                'success': False,
                'error': 'Chips insufficienti'
            })
            return
        
        # Crea il giocatore
        player = Player(
            id=conn_id,
            username=player_info['username'],
            user_id=player_info['user_id'],
            chips=buy_in
        )
        
        if table.add_player(player):
            # Aggiorna chips del giocatore
            player_info['chips'] -= buy_in
            self.db.update_chips(player_info['user_id'], -buy_in)
            
            self.lobby_users.discard(conn_id)
            
            await self.send_to(conn_id, {
                'type': 'join_result',
                'success': True,
                'table': table.to_dict(for_player_id=conn_id)
            })
            
            # Notifica gli altri
            await self.broadcast_to_table(table_id, {
                'type': 'player_joined',
                'player': player.to_dict(),
                'table': table.to_dict()
            }, exclude=conn_id)
        else:
            await self.send_to(conn_id, {
                'type': 'join_result',
                'success': False,
                'error': 'Tavolo pieno'
            })
    
    async def handle_leave_table(self, conn_id: str, message: dict):
        """Gestisce l'uscita da un tavolo"""
        table_id = message.get('table_id')
        
        if table_id not in self.tables:
            return
        
        table = self.tables[table_id]
        
        if conn_id in table.players:
            player = table.players[conn_id]
            
            # Restituisci le chips
            if conn_id in self.players:
                self.players[conn_id]['chips'] += player.chips
                self.db.update_chips(self.players[conn_id]['user_id'], player.chips)
            
            table.remove_player(conn_id)
            self.lobby_users.add(conn_id)
            
            await self.send_to(conn_id, {
                'type': 'leave_result',
                'success': True
            })
            
            await self.broadcast_to_table(table_id, {
                'type': 'player_left',
                'player_id': conn_id,
                'table': table.to_dict()
            })
    
    async def handle_start_game(self, conn_id: str, message: dict):
        """Avvia una nuova mano"""
        table_id = message.get('table_id')
        
        if table_id not in self.tables:
            return
        
        table = self.tables[table_id]
        
        if table.start_hand():
            # Invia stato a ogni giocatore (con le proprie carte)
            for player_id in table.players:
                await self.send_to(player_id, {
                    'type': 'game_started',
                    'table': table.to_dict(for_player_id=player_id)
                })
    
    async def handle_game_action(self, conn_id: str, message: dict):
        """Gestisce un'azione di gioco"""
        table_id = message.get('table_id')
        action = message.get('game_action')
        amount = message.get('amount', 0)
        
        if table_id not in self.tables:
            return
        
        table = self.tables[table_id]
        result = table.process_action(conn_id, action, amount)
        
        if result['success']:
            # Aggiorna statistiche
            if conn_id in self.players:
                user_id = self.players[conn_id]['user_id']
                self.db.update_stats(user_id, {'hands_played': 1})
            
            # Invia aggiornamento a tutti
            for player_id in table.players:
                await self.send_to(player_id, {
                    'type': 'game_update',
                    'action_result': result,
                    'table': table.to_dict(for_player_id=player_id)
                })
            
            # Se la mano è finita
            if table.state == GameState.FINISHED:
                await self._handle_hand_finished(table)
        else:
            await self.send_to(conn_id, {
                'type': 'action_error',
                'error': result['error']
            })
    
    async def _handle_hand_finished(self, table: PokerTable):
        """Gestisce la fine di una mano"""
        # Trova i vincitori
        winners = [p for p in table.players.values() if not p.is_folded]
        
        # Aggiorna statistiche
        for player in table.players.values():
            if player.id in self.players:
                user_id = self.players[player.id]['user_id']
                
                if player in winners:
                    self.db.update_stats(user_id, {'hands_won': 1})
        
        # Invia risultato
        for player_id in table.players:
            await self.send_to(player_id, {
                'type': 'hand_finished',
                'table': table.to_dict(for_player_id=player_id),
                'winners': [p.username for p in winners]
            })
        
        # Resetta per la prossima mano
        table.state = GameState.WAITING
    
    async def handle_chat(self, conn_id: str, message: dict):
        """Gestisce i messaggi di chat"""
        text = message.get('text', '').strip()
        table_id = message.get('table_id')
        
        if not text or conn_id not in self.players:
            return
        
        username = self.players[conn_id]['username']
        
        chat_message = {
            'type': 'chat',
            'username': username,
            'text': text,
            'timestamp': datetime.now().isoformat()
        }
        
        if table_id and table_id in self.tables:
            await self.broadcast_to_table(table_id, chat_message)
        else:
            await self.broadcast_lobby(chat_message)
    
    async def handle_get_stats(self, conn_id: str, message: dict):
        """Ritorna le statistiche del giocatore"""
        if conn_id not in self.players:
            return
        
        user_id = self.players[conn_id]['user_id']
        stats = self.db.get_user_stats(user_id)
        
        await self.send_to(conn_id, {
            'type': 'stats',
            'stats': stats
        })
    
    async def handle_get_leaderboard(self, conn_id: str, message: dict):
        """Ritorna la classifica"""
        limit = message.get('limit', 10)
        leaderboard = self.db.get_leaderboard(limit)
        
        await self.send_to(conn_id, {
            'type': 'leaderboard',
            'leaderboard': leaderboard
        })
    
    async def handle_get_history(self, conn_id: str, message: dict):
        """Ritorna lo storico partite"""
        if conn_id not in self.players:
            return
        
        user_id = self.players[conn_id]['user_id']
        limit = message.get('limit', 20)
        history = self.db.get_game_history(user_id, limit)
        
        await self.send_to(conn_id, {
            'type': 'history',
            'history': history
        })
    
    async def handle_get_tournaments(self, conn_id: str, message: dict):
        """Ritorna la lista dei tornei"""
        tournaments_info = []
        
        for tournament in self.tournaments.values():
            tournaments_info.append(tournament.to_dict())
        
        await self.send_to(conn_id, {
            'type': 'tournaments_list',
            'tournaments': tournaments_info
        })
    
    async def handle_create_tournament(self, conn_id: str, message: dict):
        """Crea un nuovo torneo"""
        name = message.get('name', 'Torneo')
        buy_in = message.get('buy_in', 100)
        starting_chips = message.get('starting_chips', 5000)
        max_players = message.get('max_players', 100)
        
        tournament_id = self._generate_id("tour_")
        
        tournament = Tournament(
            tournament_id=tournament_id,
            name=name,
            buy_in=buy_in,
            starting_chips=starting_chips,
            max_players=max_players
        )
        
        self.tournaments[tournament_id] = tournament
        
        await self.send_to(conn_id, {
            'type': 'tournament_created',
            'tournament': tournament.to_dict()
        })
        
        # Notifica tutti nella lobby
        await self.broadcast_lobby({
            'type': 'new_tournament',
            'tournament': tournament.to_dict()
        })
    
    async def handle_join_tournament(self, conn_id: str, message: dict):
        """Iscrivi un giocatore a un torneo"""
        tournament_id = message.get('tournament_id')
        
        if conn_id not in self.players:
            await self.send_to(conn_id, {
                'type': 'tournament_join_result',
                'success': False,
                'error': 'Devi effettuare il login'
            })
            return
        
        if tournament_id not in self.tournaments:
            await self.send_to(conn_id, {
                'type': 'tournament_join_result',
                'success': False,
                'error': 'Torneo non trovato'
            })
            return
        
        tournament = self.tournaments[tournament_id]
        player_info = self.players[conn_id]
        
        if player_info['chips'] < tournament.buy_in:
            await self.send_to(conn_id, {
                'type': 'tournament_join_result',
                'success': False,
                'error': 'Chips insufficienti per il buy-in'
            })
            return
        
        if tournament.register_player(conn_id, player_info['user_id'], player_info['username']):
            player_info['chips'] -= tournament.buy_in
            self.db.update_chips(player_info['user_id'], -tournament.buy_in)
            
            await self.send_to(conn_id, {
                'type': 'tournament_join_result',
                'success': True,
                'tournament': tournament.to_dict()
            })
            
            # Notifica aggiornamento
            await self.broadcast_lobby({
                'type': 'tournament_update',
                'tournament': tournament.to_dict()
            })
        else:
            await self.send_to(conn_id, {
                'type': 'tournament_join_result',
                'success': False,
                'error': 'Impossibile iscriversi al torneo'
            })
    
    async def handle_leave_tournament(self, conn_id: str, message: dict):
        """Rimuove un giocatore da un torneo"""
        tournament_id = message.get('tournament_id')
        
        if tournament_id not in self.tournaments:
            return
        
        tournament = self.tournaments[tournament_id]
        
        if tournament.unregister_player(conn_id):
            # Restituisci buy-in
            if conn_id in self.players:
                self.players[conn_id]['chips'] += tournament.buy_in
                self.db.update_chips(self.players[conn_id]['user_id'], tournament.buy_in)
            
            await self.send_to(conn_id, {
                'type': 'tournament_leave_result',
                'success': True
            })
            
            await self.broadcast_lobby({
                'type': 'tournament_update',
                'tournament': tournament.to_dict()
            })
    
    # ============================================
    # WALLET HANDLERS
    # ============================================
    
    async def handle_get_wallet(self, conn_id: str, message: dict):
        """Ottiene i dati del wallet"""
        if conn_id not in self.players:
            await self.send_to(conn_id, {
                'type': 'wallet_data',
                'success': False,
                'error': 'Non autenticato'
            })
            return
        
        user_id = self.players[conn_id]['user_id']
        wallet = self.db.get_or_create_wallet(user_id)
        transactions = self.db.get_transactions(user_id, limit=20)
        
        await self.send_to(conn_id, {
            'type': 'wallet_data',
            'success': True,
            'balance': wallet['balance'],
            'in_game': wallet['in_game'],
            'gold_tokens': wallet['gold_tokens'],
            'daily_limit': wallet['daily_limit'],
            'today_deposits': wallet['today_deposits'],
            'today_withdrawals': wallet['today_withdrawals'],
            'today_winloss': wallet['today_winloss'],
            'transactions': transactions,
            'message_id': message.get('message_id')
        })
    
    async def handle_wallet_deposit(self, conn_id: str, message: dict):
        """Creates a PayPal order for deposit - Step 1"""
        if conn_id not in self.players:
            await self.send_to(conn_id, {
                'type': 'wallet_deposit_result',
                'success': False,
                'error': 'Non autenticato',
                'message_id': message.get('message_id')
            })
            return
        
        user_id = self.players[conn_id]['user_id']
        amount = message.get('amount', 0)
        
        if amount < 1:
            await self.send_to(conn_id, {
                'type': 'wallet_deposit_result',
                'success': False,
                'error': 'Importo minimo: €1',
                'message_id': message.get('message_id')
            })
            return
        
        try:
            # Create PayPal order
            result = await paypal_api.create_order(
                amount=amount,
                currency="EUR",
                description=f"PokerTexas Deposit - €{amount:.2f}"
            )
            
            if result.get('success'):
                # Store pending order in database for later verification
                self.db.cursor.execute('''
                    INSERT INTO transactions (user_id, type, amount, method, status, paypal_order_id, details)
                    VALUES (?, 'deposit', ?, 'paypal', 'pending', ?, ?)
                ''', (user_id, amount, result['order_id'], json.dumps({'status': 'created'})))
                self.db.conn.commit()
                
                await self.send_to(conn_id, {
                    'type': 'wallet_deposit_result',
                    'success': True,
                    'order_id': result['order_id'],
                    'approval_url': result['approval_url'],
                    'amount': amount,
                    'message_id': message.get('message_id')
                })
            else:
                await self.send_to(conn_id, {
                    'type': 'wallet_deposit_result',
                    'success': False,
                    'error': result.get('error', 'Errore PayPal'),
                    'message_id': message.get('message_id')
                })
        except Exception as e:
            await self.send_to(conn_id, {
                'type': 'wallet_deposit_result',
                'success': False,
                'error': f'Errore: {str(e)}',
                'message_id': message.get('message_id')
            })
    
    async def handle_capture_deposit(self, conn_id: str, message: dict):
        """Captures (completes) a PayPal deposit after user approval - Step 2"""
        if conn_id not in self.players:
            await self.send_to(conn_id, {
                'type': 'capture_deposit_result',
                'success': False,
                'error': 'Non autenticato',
                'message_id': message.get('message_id')
            })
            return
        
        user_id = self.players[conn_id]['user_id']
        order_id = message.get('order_id', '')
        
        if not order_id:
            await self.send_to(conn_id, {
                'type': 'capture_deposit_result',
                'success': False,
                'error': 'Order ID mancante',
                'message_id': message.get('message_id')
            })
            return
        
        try:
            # Capture the payment
            result = await paypal_api.capture_order(order_id)
            
            if result.get('success') and result.get('status') == 'COMPLETED':
                amount = result.get('amount', 0)
                
                # Update wallet balance
                self.db.cursor.execute('''
                    UPDATE wallets SET balance = balance + ?, today_deposits = today_deposits + ?
                    WHERE user_id = ?
                ''', (amount, amount, user_id))
                
                # Update transaction status
                self.db.cursor.execute('''
                    UPDATE transactions SET status = 'completed', details = ?
                    WHERE paypal_order_id = ? AND user_id = ?
                ''', (json.dumps(result), order_id, user_id))
                self.db.conn.commit()
                
                # Get updated wallet
                wallet = self.db.get_or_create_wallet(user_id)
                
                await self.send_to(conn_id, {
                    'type': 'capture_deposit_result',
                    'success': True,
                    'amount': amount,
                    'new_balance': wallet['balance'],
                    'message_id': message.get('message_id')
                })
            else:
                await self.send_to(conn_id, {
                    'type': 'capture_deposit_result',
                    'success': False,
                    'error': result.get('error', 'Pagamento non completato'),
                    'message_id': message.get('message_id')
                })
        except Exception as e:
            await self.send_to(conn_id, {
                'type': 'capture_deposit_result',
                'success': False,
                'error': f'Errore: {str(e)}',
                'message_id': message.get('message_id')
            })
    
    async def handle_wallet_withdraw(self, conn_id: str, message: dict):
        """Processes a real withdrawal via PayPal Payouts API"""
        if conn_id not in self.players:
            await self.send_to(conn_id, {
                'type': 'wallet_withdraw_result',
                'success': False,
                'error': 'Non autenticato',
                'message_id': message.get('message_id')
            })
            return
        
        user_id = self.players[conn_id]['user_id']
        amount = message.get('amount', 0)
        paypal_email = message.get('paypal_email', '')
        
        if not paypal_email:
            await self.send_to(conn_id, {
                'type': 'wallet_withdraw_result',
                'success': False,
                'error': 'Email PayPal richiesta',
                'message_id': message.get('message_id')
            })
            return
        
        if amount < 5:
            await self.send_to(conn_id, {
                'type': 'wallet_withdraw_result',
                'success': False,
                'error': 'Importo minimo prelievo: €5',
                'message_id': message.get('message_id')
            })
            return
        
        # Check balance
        wallet = self.db.get_or_create_wallet(user_id)
        if wallet['balance'] < amount:
            await self.send_to(conn_id, {
                'type': 'wallet_withdraw_result',
                'success': False,
                'error': 'Saldo insufficiente',
                'message_id': message.get('message_id')
            })
            return
        
        try:
            # Send actual payout via PayPal
            result = await paypal_api.send_payout(
                recipient_email=paypal_email,
                amount=amount,
                currency="EUR",
                note=f"PokerTexas Prelievo - €{amount:.2f}"
            )
            
            if result.get('success'):
                # Deduct from wallet
                self.db.cursor.execute('''
                    UPDATE wallets SET balance = balance - ?, today_withdrawals = today_withdrawals + ?
                    WHERE user_id = ?
                ''', (amount, amount, user_id))
                
                # Record transaction
                self.db.cursor.execute('''
                    INSERT INTO transactions (user_id, type, amount, method, status, paypal_email, details)
                    VALUES (?, 'withdrawal', ?, 'paypal', 'completed', ?, ?)
                ''', (user_id, -amount, paypal_email, json.dumps(result)))
                self.db.conn.commit()
                
                # Get updated wallet
                wallet = self.db.get_or_create_wallet(user_id)
                
                await self.send_to(conn_id, {
                    'type': 'wallet_withdraw_result',
                    'success': True,
                    'amount': amount,
                    'new_balance': wallet['balance'],
                    'payout_id': result.get('payout_batch_id'),
                    'message_id': message.get('message_id')
                })
            else:
                await self.send_to(conn_id, {
                    'type': 'wallet_withdraw_result',
                    'success': False,
                    'error': result.get('error', 'Errore invio pagamento'),
                    'message_id': message.get('message_id')
                })
        except Exception as e:
            await self.send_to(conn_id, {
                'type': 'wallet_withdraw_result',
                'success': False,
                'error': f'Errore: {str(e)}',
                'message_id': message.get('message_id')
            })
    
    async def handle_get_transactions(self, conn_id: str, message: dict):
        """Ottiene lo storico transazioni"""
        if conn_id not in self.players:
            return
        
        user_id = self.players[conn_id]['user_id']
        limit = message.get('limit', 50)
        
        transactions = self.db.get_transactions(user_id, limit)
        
        await self.send_to(conn_id, {
            'type': 'transactions_data',
            'transactions': transactions,
            'message_id': message.get('message_id')
        })
    
    # ============================================
    # PRIVATE GAMES HANDLERS
    # ============================================
    
    async def handle_create_private_tournament(self, conn_id: str, message: dict):
        """Crea un torneo privato"""
        if conn_id not in self.players:
            return
        
        tournament_data = message.get('tournament', {})
        
        # In a full implementation, save to database and create tournament
        await self.send_to(conn_id, {
            'type': 'private_tournament_created',
            'success': True,
            'tournament_id': tournament_data.get('id'),
            'message_id': message.get('message_id')
        })
    
    async def handle_create_private_cashgame(self, conn_id: str, message: dict):
        """Crea un cash game privato"""
        if conn_id not in self.players:
            return
        
        cashgame_data = message.get('cashgame', {})
        
        # In a full implementation, save to database and create table
        await self.send_to(conn_id, {
            'type': 'private_cashgame_created',
            'success': True,
            'game_id': cashgame_data.get('id'),
            'message_id': message.get('message_id')
        })
    
    async def handle_join_private_game(self, conn_id: str, message: dict):
        """Unisciti a una partita privata"""
        game_name = message.get('game_name', '')
        password = message.get('password', '')
        
        if conn_id not in self.players:
            await self.send_to(conn_id, {
                'type': 'join_private_result',
                'success': False,
                'error': 'Devi essere loggato'
            })
            return
        
        user_id = self.players[conn_id]['user_id']
        result = self.db.join_private_game(user_id, game_name, password)
        
        await self.send_to(conn_id, {
            'type': 'join_private_result',
            **result,
            'message_id': message.get('message_id')
        })
    
    async def handle_get_friend_games(self, conn_id: str, message: dict):
        """Ottiene la lista delle partite tra amici"""
        if conn_id not in self.players:
            await self.send_to(conn_id, {
                'type': 'friend_games_list',
                'games': []
            })
            return
        
        user_id = self.players[conn_id]['user_id']
        games = self.db.get_private_games(user_id)
        
        await self.send_to(conn_id, {
            'type': 'friend_games_list',
            'games': games
        })
    
    async def handle_create_friend_game(self, conn_id: str, message: dict):
        """Crea una nuova partita tra amici"""
        if conn_id not in self.players:
            await self.send_to(conn_id, {
                'type': 'friend_game_created',
                'success': False,
                'error': 'Devi essere loggato'
            })
            return
        
        user_id = self.players[conn_id]['user_id']
        name = message.get('name', '')
        password = message.get('password', '')
        game_type = message.get('game_type', 'cashgame')
        
        if not name or not password:
            await self.send_to(conn_id, {
                'type': 'friend_game_created',
                'success': False,
                'error': 'Nome e password sono obbligatori'
            })
            return
        
        settings = {
            'buy_in': message.get('buy_in', 10),
            'max_players': message.get('max_players', 6),
            'small_blind': message.get('small_blind', 1),
            'big_blind': message.get('big_blind', 2),
            'blind_increase_minutes': message.get('blind_increase_minutes', 10),
            'max_buy_in': message.get('max_buy_in', 100),
            'action_timer': message.get('action_timer', 30)
        }
        
        result = self.db.create_private_game(user_id, name, password, game_type, settings)
        
        await self.send_to(conn_id, {
            'type': 'friend_game_created',
            **result
        })
    
    async def handle_join_friend_game(self, conn_id: str, message: dict):
        """Unisciti a una partita tra amici"""
        if conn_id not in self.players:
            await self.send_to(conn_id, {
                'type': 'friend_game_joined',
                'success': False,
                'error': 'Devi essere loggato'
            })
            return
        
        user_id = self.players[conn_id]['user_id']
        game_name = message.get('game_name', '')
        password = message.get('password', '')
        
        if not game_name or not password:
            await self.send_to(conn_id, {
                'type': 'friend_game_joined',
                'success': False,
                'error': 'Nome e password sono obbligatori'
            })
            return
        
        result = self.db.join_private_game(user_id, game_name, password)
        
        await self.send_to(conn_id, {
            'type': 'friend_game_joined',
            **result
        })
    
    async def handle_check_active_table(self, conn_id: str, message: dict):
        """Controlla se l'utente ha un tavolo attivo da riconnettersi"""
        if conn_id not in self.players:
            await self.send_to(conn_id, {
                'type': 'active_table_check',
                'has_active_table': False,
                'error': 'Devi essere loggato'
            })
            return
        
        user_id = self.players[conn_id]['user_id']
        
        # Pulisci tavoli scaduti (più di 5 minuti)
        current_time = time.time()
        expired = [uid for uid, info in self.user_active_tables.items() 
                   if current_time - info.get('disconnected_at', 0) > 300]  # 5 minuti
        for uid in expired:
            # Rimuovi giocatore dal tavolo
            info = self.user_active_tables[uid]
            table_id = info['table_id']
            if table_id in self.tables:
                table = self.tables[table_id]
                for pid, player in list(table.players.items()):
                    if player.user_id == uid:
                        table.remove_player(pid)
                        print(f"[RECONNECT] Rimosso giocatore scaduto {uid} dal tavolo {table_id}")
            del self.user_active_tables[uid]
        
        # Controlla se utente ha tavolo attivo
        if user_id in self.user_active_tables:
            info = self.user_active_tables[user_id]
            table_id = info['table_id']
            
            if table_id in self.tables:
                table = self.tables[table_id]
                await self.send_to(conn_id, {
                    'type': 'active_table_check',
                    'has_active_table': True,
                    'table_id': table_id,
                    'table_name': table.name,
                    'stack': info['stack'],
                    'seat': info['seat'],
                    'small_blind': table.small_blind,
                    'big_blind': table.big_blind
                })
                return
        
        await self.send_to(conn_id, {
            'type': 'active_table_check',
            'has_active_table': False
        })
    
    async def handle_reconnect_table(self, conn_id: str, message: dict):
        """Riconnetti l'utente al tavolo precedente"""
        if conn_id not in self.players:
            await self.send_to(conn_id, {
                'type': 'table_reconnected',
                'success': False,
                'error': 'Devi essere loggato'
            })
            return
        
        user_id = self.players[conn_id]['user_id']
        username = self.players[conn_id]['username']
        
        if user_id not in self.user_active_tables:
            await self.send_to(conn_id, {
                'type': 'table_reconnected',
                'success': False,
                'error': 'Nessun tavolo attivo trovato'
            })
            return
        
        info = self.user_active_tables[user_id]
        table_id = info['table_id']
        saved_stack = info['stack']
        saved_seat = info['seat']
        
        if table_id not in self.tables:
            del self.user_active_tables[user_id]
            await self.send_to(conn_id, {
                'type': 'table_reconnected',
                'success': False,
                'error': 'Il tavolo non esiste più'
            })
            return
        
        table = self.tables[table_id]
        
        # Trova il vecchio player e aggiornalo
        old_conn_id = None
        for pid, player in table.players.items():
            if player.user_id == user_id:
                old_conn_id = pid
                break
        
        if old_conn_id:
            # Rimuovi vecchio player
            old_player = table.players.pop(old_conn_id)
            
            # Crea nuovo player con stesso stato
            new_player = Player(
                id=conn_id,
                username=username,
                user_id=user_id,
                chips=saved_stack,
                seat=saved_seat
            )
            new_player.is_connected = True
            new_player.cards = old_player.cards
            new_player.current_bet = old_player.current_bet
            new_player.is_folded = old_player.is_folded
            new_player.is_all_in = old_player.is_all_in
            
            table.players[conn_id] = new_player
        else:
            # Player non trovato, creane uno nuovo
            new_player = Player(
                id=conn_id,
                username=username,
                user_id=user_id,
                chips=saved_stack,
                seat=saved_seat
            )
            table.players[conn_id] = new_player
        
        # Rimuovi da tracking
        del self.user_active_tables[user_id]
        
        print(f"[RECONNECT] User {user_id} riconnesso al tavolo {table_id} con stack {saved_stack}")
        
        # Invia stato tavolo
        await self.send_to(conn_id, {
            'type': 'table_reconnected',
            'success': True,
            'table_id': table_id,
            'table_name': table.name,
            'stack': saved_stack,
            'seat': saved_seat,
            'game_state': table.state.value,
            'pot': table.pot,
            'community_cards': [c.to_dict() for c in table.community_cards],
            'players': [p.to_dict() for p in table.players.values()],
            'small_blind': table.small_blind,
            'big_blind': table.big_blind
        })
        
        # Notifica altri giocatori
        await self.broadcast_table(table_id, {
            'type': 'player_reconnected',
            'player_id': conn_id,
            'username': username,
            'seat': saved_seat
        }, exclude=conn_id)
    
    async def handler(self, websocket: websockets.WebSocketServerProtocol):
        """Handler principale per le connessioni WebSocket"""
        conn_id = await self.register(websocket)
        
        try:
            await self.send_to(conn_id, {
                'type': 'connected',
                'connection_id': conn_id,
                'message': 'Benvenuto al server Texas Hold\'em!'
            })
            
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self.handle_message(conn_id, data)
                except json.JSONDecodeError:
                    await self.send_to(conn_id, {
                        'type': 'error',
                        'message': 'Messaggio non valido'
                    })
        
        finally:
            await self.unregister(conn_id)
    
    async def start(self):
        """Avvia il server"""
        print(f"""
╔══════════════════════════════════════════════════════════════╗
║        🃏 TEXAS HOLD'EM POKER SERVER ONLINE 🃏               ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  Server avviato su: ws://{SERVER_HOST}:{SERVER_PORT}                   ║
║                                                              ║
║  Caratteristiche:                                            ║
║  • Multiplayer online                                        ║
║  • Sistema di registrazione/login                            ║
║  • Statistiche e classifiche                                 ║
║  • Tornei multi-tavolo                                       ║
║  • Chat in tempo reale                                       ║
║                                                              ║
║  Premi Ctrl+C per fermare il server                         ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
        """)
        
        async with websockets.serve(self.handler, SERVER_HOST, SERVER_PORT):
            await asyncio.Future()  # Run forever


# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    server = PokerServer()
    
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("\n\n👋 Server fermato. Arrivederci!")
