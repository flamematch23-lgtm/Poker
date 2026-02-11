#!/usr/bin/env python3
"""
Poker Texas Hold'em Server v13 - Password Recovery
- Complete password recovery via security questions
- Email field in users table
- All v12 features included
"""

import asyncio
import json
import hashlib
import os
import time
from datetime import datetime
import aiohttp
import aiosqlite
import websockets
from websockets.exceptions import ConnectionClosed

# PayPal Configuration
PAYPAL_CLIENT_ID = os.environ.get('PAYPAL_CLIENT_ID', 'ATGUiTFJ0G6kKrJ4RYJ0sg80pZ3qlTqK8WFkIieVu2fU0X354vLFsyel8QVKleajel1ZpgslVsliuVAI')
PAYPAL_SECRET = os.environ.get('PAYPAL_SECRET', 'EPsoCGBkuF3LI8KQKbTWBDhjw6f4gc2RUscrAw9W3baDJlU-0ZyKnuU6qVmAnGbzmn12AcMNcbRRYGgB')
PAYPAL_API_BASE = "https://api-m.sandbox.paypal.com"

# Security Questions (5 options)
SECURITY_QUESTIONS = [
    "Qual è il nome del tuo primo animale domestico?",
    "Qual è il cognome da nubile di tua madre?",
    "In quale città sei nato?",
    "Qual è il tuo film preferito?",
    "Qual è il nome della tua scuola elementare?"
]

class PayPalClient:
    def __init__(self):
        self.access_token = None
        self.token_expires = 0
    
    async def get_access_token(self) -> str:
        if self.access_token and time.time() < self.token_expires:
            return self.access_token
        
        auth = aiohttp.BasicAuth(PAYPAL_CLIENT_ID, PAYPAL_SECRET)
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{PAYPAL_API_BASE}/v1/oauth2/token",
                auth=auth,
                data={"grant_type": "client_credentials"}
            ) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    raise Exception(f"PayPal auth failed: {error}")
                data = await resp.json()
                self.access_token = data["access_token"]
                self.token_expires = time.time() + data["expires_in"] - 60
                return self.access_token
    
    async def create_order(self, amount: float, currency: str = "EUR",
                          description: str = "Poker Chips Deposit") -> dict:
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
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {token}"
                },
                json=order_data
            ) as resp:
                if resp.status not in (200, 201):
                    error = await resp.text()
                    raise Exception(f"PayPal order failed: {error}")
                return await resp.json()
    
    async def capture_order(self, order_id: str) -> dict:
        token = await self.get_access_token()
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{PAYPAL_API_BASE}/v2/checkout/orders/{order_id}/capture",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {token}"
                }
            ) as resp:
                return await resp.json()
    
    async def get_order(self, order_id: str) -> dict:
        token = await self.get_access_token()
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{PAYPAL_API_BASE}/v2/checkout/orders/{order_id}",
                headers={
                    "Authorization": f"Bearer {token}"
                }
            ) as resp:
                return await resp.json()
    
    async def create_payout(self, email: str, amount: float, currency: str = "EUR") -> dict:
        token = await self.get_access_token()
        
        payout_data = {
            "sender_batch_header": {
                "sender_batch_id": f"Payout_{int(time.time())}",
                "email_subject": "PokerTexas - Prelievo",
                "email_message": "Hai ricevuto un pagamento da PokerTexas"
            },
            "items": [{
                "recipient_type": "EMAIL",
                "amount": {
                    "value": f"{amount:.2f}",
                    "currency": currency
                },
                "receiver": email,
                "note": "Prelievo PokerTexas"
            }]
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{PAYPAL_API_BASE}/v1/payments/payouts",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {token}"
                },
                json=payout_data
            ) as resp:
                return await resp.json()

class PokerTable:
    def __init__(self, table_id: str, name: str, small_blind: float, big_blind: float, 
                 min_buy_in: float, max_buy_in: float, max_players: int = 6):
        self.table_id = table_id
        self.name = name
        self.small_blind = small_blind
        self.big_blind = big_blind
        self.min_buy_in = min_buy_in
        self.max_buy_in = max_buy_in
        self.max_players = max_players
        self.players = {}  # user_id -> {username, chips, position, is_active, is_sitting_out}
        self.spectators = set()
        self.dealer_position = 0
        self.current_player = None
        self.pot = 0.0
        self.community_cards = []
        self.game_phase = "waiting"  # waiting, preflop, flop, turn, river, showdown
        self.current_bet = 0.0
        self.last_action_time = None
        self.is_private = False
        self.password = None
    
    def add_player(self, user_id: int, username: str, chips: float, position: int = None):
        if len(self.players) >= self.max_players:
            return False, "Table is full"
        
        if position is None:
            # Find first available position
            taken = {p['position'] for p in self.players.values()}
            for i in range(self.max_players):
                if i not in taken:
                    position = i
                    break
        
        self.players[user_id] = {
            'username': username,
            'chips': chips,
            'position': position,
            'is_active': True,
            'is_sitting_out': False,
            'cards': [],
            'current_bet': 0.0
        }
        return True, position
    
    def remove_player(self, user_id: int):
        if user_id in self.players:
            chips = self.players[user_id]['chips']
            del self.players[user_id]
            return chips
        return 0
    
    def get_state(self, for_user_id: int = None):
        players_state = []
        for uid, p in self.players.items():
            player_state = {
                'user_id': uid,
                'username': p['username'],
                'chips': p['chips'],
                'position': p['position'],
                'is_active': p['is_active'],
                'is_sitting_out': p['is_sitting_out'],
                'current_bet': p['current_bet'],
                'has_cards': len(p['cards']) > 0
            }
            # Only show cards to the player themselves
            if for_user_id == uid:
                player_state['cards'] = p['cards']
            players_state.append(player_state)
        
        return {
            'table_id': self.table_id,
            'name': self.name,
            'small_blind': self.small_blind,
            'big_blind': self.big_blind,
            'min_buy_in': self.min_buy_in,
            'max_buy_in': self.max_buy_in,
            'max_players': self.max_players,
            'players': players_state,
            'dealer_position': self.dealer_position,
            'current_player': self.current_player,
            'pot': self.pot,
            'community_cards': self.community_cards,
            'game_phase': self.game_phase,
            'current_bet': self.current_bet
        }

class PokerServer:
    def __init__(self):
        self.connections = {}  # websocket -> user_id
        self.user_connections = {}  # user_id -> websocket
        self.paypal = PayPalClient()
        self.db_path = "poker_database.db"
        self.tables = {}  # table_id -> PokerTable
        self.user_tables = {}  # user_id -> table_id (active table)
        self._init_default_tables()
    
    def _init_default_tables(self):
        # Create default cash game tables with cent-based blinds
        default_tables = [
            ("table_micro", "Micro Stakes", 0.05, 0.10, 2.0, 10.0),
            ("table_low", "Low Stakes", 0.10, 0.20, 4.0, 20.0),
            ("table_medium", "Medium Stakes", 0.25, 0.50, 10.0, 50.0),
            ("table_high", "High Stakes", 0.50, 1.00, 20.0, 100.0),
            ("table_vip", "VIP Room", 1.00, 2.00, 40.0, 200.0),
        ]
        for table_id, name, sb, bb, min_buy, max_buy in default_tables:
            self.tables[table_id] = PokerTable(table_id, name, sb, bb, min_buy, max_buy)
    
    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            # Users table with email and security question
            await db.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT UNIQUE NOT NULL,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    security_question INTEGER NOT NULL,
                    security_answer TEXT NOT NULL,
                    chips INTEGER DEFAULT 10000,
                    level INTEGER DEFAULT 1,
                    avatar_id INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP
                )
            ''')
            
            # Statistics table
            await db.execute('''
                CREATE TABLE IF NOT EXISTS statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    games_played INTEGER DEFAULT 0,
                    games_won INTEGER DEFAULT 0,
                    chips_won INTEGER DEFAULT 0,
                    chips_lost INTEGER DEFAULT 0,
                    royal_flush INTEGER DEFAULT 0,
                    straight_flush INTEGER DEFAULT 0,
                    four_of_kind INTEGER DEFAULT 0,
                    full_house INTEGER DEFAULT 0,
                    flush INTEGER DEFAULT 0,
                    straight INTEGER DEFAULT 0,
                    three_of_kind INTEGER DEFAULT 0,
                    two_pair INTEGER DEFAULT 0,
                    pair INTEGER DEFAULT 0,
                    high_card INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )
            ''')
            
            # Game history
            await db.execute('''
                CREATE TABLE IF NOT EXISTS game_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    game_type TEXT NOT NULL,
                    result TEXT NOT NULL,
                    chips_change INTEGER,
                    hand TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )
            ''')
            
            # Wallets
            await db.execute('''
                CREATE TABLE IF NOT EXISTS wallets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE NOT NULL,
                    balance REAL DEFAULT 0.0,
                    total_deposited REAL DEFAULT 0.0,
                    total_withdrawn REAL DEFAULT 0.0,
                    last_deposit TIMESTAMP,
                    last_withdrawal TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )
            ''')
            
            # Transactions
            await db.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    type TEXT NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT DEFAULT 'pending',
                    paypal_order_id TEXT,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )
            ''')
            
            # Friends
            await db.execute('''
                CREATE TABLE IF NOT EXISTS friends (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    friend_id INTEGER NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id),
                    FOREIGN KEY (friend_id) REFERENCES users(id),
                    UNIQUE(user_id, friend_id)
                )
            ''')
            
            # Private games
            await db.execute('''
                CREATE TABLE IF NOT EXISTS private_games (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    creator_id INTEGER NOT NULL,
                    game_name TEXT NOT NULL,
                    password TEXT NOT NULL,
                    game_type TEXT DEFAULT 'cash',
                    small_blind REAL DEFAULT 0.10,
                    big_blind REAL DEFAULT 0.20,
                    min_buy_in REAL DEFAULT 5.0,
                    max_buy_in REAL DEFAULT 50.0,
                    max_players INTEGER DEFAULT 6,
                    status TEXT DEFAULT 'waiting',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (creator_id) REFERENCES users(id)
                )
            ''')
            
            await db.commit()
            print("Database initialized with v13 schema (password recovery)")
    
    def hash_password(self, password: str) -> str:
        return hashlib.sha256(password.encode()).hexdigest()
    
    async def handle_register(self, ws, data: dict):
        email = data.get('email', '').strip().lower()
        username = data.get('username', '').strip()
        password = data.get('password', '')
        security_question = data.get('security_question', 0)
        security_answer = data.get('security_answer', '').strip().lower()
        
        if not email or '@' not in email:
            return {"type": "register_result", "success": False, "error": "Email non valida"}
        if not username or len(username) < 3:
            return {"type": "register_result", "success": False, "error": "Username deve avere almeno 3 caratteri"}
        if not password or len(password) < 6:
            return {"type": "register_result", "success": False, "error": "Password deve avere almeno 6 caratteri"}
        if not security_question or not isinstance(security_question, str):
            return {"type": "register_result", "success": False, "error": "Domanda di sicurezza non valida"}
        if not security_answer or len(security_answer) < 2:
            return {"type": "register_result", "success": False, "error": "Risposta di sicurezza non valida"}
        
        async with aiosqlite.connect(self.db_path) as db:
            # Check email uniqueness
            cursor = await db.execute("SELECT id FROM users WHERE email = ?", (email,))
            if await cursor.fetchone():
                return {"type": "register_result", "success": False, "error": "Email già registrata"}
            
            # Check username uniqueness
            cursor = await db.execute("SELECT id FROM users WHERE username = ?", (username,))
            if await cursor.fetchone():
                return {"type": "register_result", "success": False, "error": "Username già in uso"}
            
            # Create user
            password_hash = self.hash_password(password)
            cursor = await db.execute(
                """INSERT INTO users (email, username, password_hash, security_question, security_answer, chips, level)
                   VALUES (?, ?, ?, ?, ?, 10000, 1)""",
                (email, username, password_hash, security_question, security_answer)
            )
            user_id = cursor.lastrowid
            
            # Create statistics
            await db.execute("INSERT INTO statistics (user_id) VALUES (?)", (user_id,))
            
            # Create wallet
            await db.execute("INSERT INTO wallets (user_id, balance) VALUES (?, 0.0)", (user_id,))
            
            await db.commit()
            
            return {
                "type": "register_result",
                "success": True,
                "user_id": user_id,
                "username": username,
                "message": "Registrazione completata!"
            }
    
    async def handle_ping(self, ws, data: dict):
        return {"type": "pong"}

    async def handle_login(self, ws, data: dict):
        email = data.get('email', '').strip().lower()
        password = data.get('password', '')
        
        if not email or not password:
            return {"type": "login_result", "success": False, "error": "Email e password richiesti"}
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT id, username, chips, level, avatar_id FROM users WHERE email = ? AND password_hash = ?",
                (email, self.hash_password(password))
            )
            user = await cursor.fetchone()
            
            if not user:
                return {"type": "login_result", "success": False, "error": "Credenziali non valide"}
            
            user_id = user['id']
            
            # Update last login
            await db.execute("UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = ?", (user_id,))
            await db.commit()
            
            # Get wallet balance
            cursor = await db.execute("SELECT balance FROM wallets WHERE user_id = ?", (user_id,))
            wallet = await cursor.fetchone()
            balance = wallet['balance'] if wallet else 0.0
            
            # Get statistics for level calculation
            cursor = await db.execute("SELECT games_played FROM statistics WHERE user_id = ?", (user_id,))
            stats = await cursor.fetchone()
            games_played = stats['games_played'] if stats else 0
            level = max(1, games_played // 10 + 1)
            
            # Store connection
            self.connections[ws] = user_id
            self.user_connections[user_id] = ws
            
            return {
                "type": "login_result",
                "success": True,
                "user_id": user_id,
                "username": user['username'],
                "chips": user['chips'],
                "level": level,
                "avatar_id": user['avatar_id'] if 'avatar_id' in user.keys() else 0,
                "wallet_balance": balance,
                "message": "Login effettuato!"
            }
    
    async def handle_get_security_question(self, ws, data: dict):
        """Get security question for password recovery - Step 1"""
        email = data.get('email', '').strip().lower()
        
        if not email or '@' not in email:
            return {"type": "security_question_response", "success": False, "error": "Email non valida"}
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT security_question FROM users WHERE email = ?",
                (email,)
            )
            user = await cursor.fetchone()
            
            if not user:
                return {"type": "security_question_response", "success": False, "error": "Email non trovata"}
            
            question_index = user['security_question']
            question_text = SECURITY_QUESTIONS[question_index] if 0 <= question_index < len(SECURITY_QUESTIONS) else "Domanda non disponibile"
            
            return {
                "type": "security_question_response",
                "success": True,
                "email": email,
                "question_index": question_index,
                "question": question_text
            }
    
    async def handle_verify_security_answer(self, ws, data: dict):
        """Verify security answer - Step 2"""
        email = data.get('email', '').strip().lower()
        answer = data.get('answer', '').strip().lower()
        
        if not email or not answer:
            return {"type": "verify_answer_response", "success": False, "error": "Email e risposta richiesti"}
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT id, security_answer FROM users WHERE email = ?",
                (email,)
            )
            user = await cursor.fetchone()
            
            if not user:
                return {"type": "verify_answer_response", "success": False, "error": "Email non trovata"}
            
            if user['security_answer'].lower() != answer:
                return {"type": "verify_answer_response", "success": False, "error": "Risposta non corretta"}
            
            return {
                "type": "verify_answer_response",
                "success": True,
                "email": email,
                "message": "Risposta corretta! Puoi reimpostare la password."
            }
    
    async def handle_reset_password(self, ws, data: dict):
        """Reset password after security verification - Step 3"""
        email = data.get('email', '').strip().lower()
        answer = data.get('answer', '').strip().lower()
        new_password = data.get('new_password', '')
        
        if not email or not answer or not new_password:
            return {"type": "reset_password_response", "success": False, "error": "Tutti i campi sono richiesti"}
        
        if len(new_password) < 6:
            return {"type": "reset_password_response", "success": False, "error": "La password deve avere almeno 6 caratteri"}
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT id, security_answer FROM users WHERE email = ?",
                (email,)
            )
            user = await cursor.fetchone()
            
            if not user:
                return {"type": "reset_password_response", "success": False, "error": "Email non trovata"}
            
            if user['security_answer'].lower() != answer:
                return {"type": "reset_password_response", "success": False, "error": "Risposta di sicurezza non corretta"}
            
            # Update password
            new_hash = self.hash_password(new_password)
            await db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (new_hash, user['id']))
            await db.commit()
            
            return {
                "type": "reset_password_response",
                "success": True,
                "message": "Password reimpostata con successo! Ora puoi effettuare il login."
            }
    
    async def handle_get_wallet(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "wallet_data", "success": False, "error": "Non autenticato"}
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT balance, total_deposited, total_withdrawn FROM wallets WHERE user_id = ?",
                (user_id,)
            )
            wallet = await cursor.fetchone()
            
            # Get recent transactions
            cursor = await db.execute(
                """SELECT type, amount, status, description, created_at 
                   FROM transactions WHERE user_id = ? 
                   ORDER BY created_at DESC LIMIT 20""",
                (user_id,)
            )
            transactions = await cursor.fetchall()
            
            return {
                "type": "wallet_data",
                "success": True,
                "balance": wallet['balance'] if wallet else 0.0,
                "total_deposited": wallet['total_deposited'] if wallet else 0.0,
                "total_withdrawn": wallet['total_withdrawn'] if wallet else 0.0,
                "transactions": [dict(t) for t in transactions]
            }
    
    async def handle_create_deposit(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "wallet_deposit_result", "success": False, "error": "Non autenticato"}
        
        amount = data.get('amount', 0)
        if amount < 1:
            return {"type": "wallet_deposit_result", "success": False, "error": "Importo minimo: €1"}
        if amount > 1000:
            return {"type": "wallet_deposit_result", "success": False, "error": "Importo massimo: €1000"}
        
        try:
            order = await self.paypal.create_order(amount)
            order_id = order['id']
            
            # Find approval URL
            approval_url = None
            for link in order.get('links', []):
                if link.get('rel') == 'approve':
                    approval_url = link['href']
                    break
            
            # Store transaction
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """INSERT INTO transactions (user_id, type, amount, status, paypal_order_id, description)
                       VALUES (?, 'deposit', ?, 'pending', ?, 'PayPal Deposit')""",
                    (user_id, amount, order_id)
                )
                await db.commit()
            
            return {
                "type": "wallet_deposit_result",
                "success": True,
                "order_id": order_id,
                "approval_url": approval_url,
                "amount": amount
            }
        except Exception as e:
            return {"type": "wallet_deposit_result", "success": False, "error": str(e)}
    
    async def handle_verify_deposit(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "capture_deposit_result", "success": False, "error": "Non autenticato"}
        
        order_id = data.get('order_id')
        if not order_id:
            return {"type": "capture_deposit_result", "success": False, "error": "Order ID mancante"}
        
        try:
            # Check order status
            order = await self.paypal.get_order(order_id)
            status = order.get('status')
            
            if status == 'APPROVED':
                # Capture the payment
                capture = await self.paypal.capture_order(order_id)
                if capture.get('status') == 'COMPLETED':
                    status = 'COMPLETED'
            
            if status == 'COMPLETED':
                async with aiosqlite.connect(self.db_path) as db:
                    # Get transaction
                    cursor = await db.execute(
                        "SELECT amount FROM transactions WHERE paypal_order_id = ? AND user_id = ?",
                        (order_id, user_id)
                    )
                    tx = await cursor.fetchone()
                    if tx:
                        amount = tx[0]
                        
                        # Update wallet
                        await db.execute(
                            """UPDATE wallets SET 
                               balance = balance + ?,
                               total_deposited = total_deposited + ?,
                               last_deposit = CURRENT_TIMESTAMP
                               WHERE user_id = ?""",
                            (amount, amount, user_id)
                        )
                        
                        # Update transaction
                        await db.execute(
                            """UPDATE transactions SET status = 'completed', completed_at = CURRENT_TIMESTAMP
                               WHERE paypal_order_id = ?""",
                            (order_id,)
                        )
                        
                        await db.commit()
                        
                        # Get new balance
                        cursor = await db.execute("SELECT balance FROM wallets WHERE user_id = ?", (user_id,))
                        wallet = await cursor.fetchone()
                        
                        return {
                            "type": "capture_deposit_result",
                            "success": True,
                            "amount": amount,
                            "new_balance": wallet[0] if wallet else amount,
                            "message": f"Deposito di €{amount:.2f} completato!"
                        }
            
            return {
                "type": "capture_deposit_result",
                "success": False,
                "status": status,
                "error": "Pagamento non completato"
            }
        except Exception as e:
            return {"type": "capture_deposit_result", "success": False, "error": str(e)}
    
    async def handle_withdraw(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "wallet_withdraw_result", "success": False, "error": "Non autenticato"}
        
        amount = data.get('amount', 0)
        paypal_email = data.get('paypal_email', '').strip()
        
        if amount < 10:
            return {"type": "wallet_withdraw_result", "success": False, "error": "Importo minimo: €10"}
        if not paypal_email or '@' not in paypal_email:
            return {"type": "wallet_withdraw_result", "success": False, "error": "Email PayPal non valida"}
        
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT balance FROM wallets WHERE user_id = ?", (user_id,))
            wallet = await cursor.fetchone()
            
            if not wallet or wallet[0] < amount:
                return {"type": "wallet_withdraw_result", "success": False, "error": "Saldo insufficiente"}
            
            try:
                payout = await self.paypal.create_payout(paypal_email, amount)
                
                if 'batch_header' in payout:
                    # Deduct from wallet
                    await db.execute(
                        """UPDATE wallets SET 
                           balance = balance - ?,
                           total_withdrawn = total_withdrawn + ?,
                           last_withdrawal = CURRENT_TIMESTAMP
                           WHERE user_id = ?""",
                        (amount, amount, user_id)
                    )
                    
                    # Record transaction
                    await db.execute(
                        """INSERT INTO transactions (user_id, type, amount, status, description)
                           VALUES (?, 'withdrawal', ?, 'completed', ?)""",
                        (user_id, amount, f"PayPal: {paypal_email}")
                    )
                    
                    await db.commit()
                    
                    # Get new balance
                    cursor = await db.execute("SELECT balance FROM wallets WHERE user_id = ?", (user_id,))
                    new_wallet = await cursor.fetchone()
                    
                    return {
                        "type": "wallet_withdraw_result",
                        "success": True,
                        "amount": amount,
                        "new_balance": new_wallet[0] if new_wallet else 0,
                        "message": f"Prelievo di €{amount:.2f} inviato a {paypal_email}"
                    }
                else:
                    return {"type": "wallet_withdraw_result", "success": False, "error": "Errore PayPal"}
            except Exception as e:
                return {"type": "wallet_withdraw_result", "success": False, "error": str(e)}
    
    async def handle_get_statistics(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "stats_data", "success": False, "error": "Non autenticato"}
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM statistics WHERE user_id = ?", (user_id,))
            stats = await cursor.fetchone()
            
            if stats:
                return {
                    "type": "stats_data",
                    "success": True,
                    "statistics": dict(stats)
                }
            return {"type": "stats_data", "success": True, "statistics": {}}
    
    async def handle_search_users(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "search_results", "success": False, "error": "Non autenticato"}
        
        query = data.get('query', '').strip()
        if len(query) < 2:
            return {"type": "search_results", "success": False, "error": "Inserisci almeno 2 caratteri"}
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT id, username, level FROM users 
                   WHERE username LIKE ? AND id != ? LIMIT 20""",
                (f"%{query}%", user_id)
            )
            users = await cursor.fetchall()
            
            return {
                "type": "search_results",
                "success": True,
                "users": [dict(u) for u in users]
            }
    
    async def handle_send_friend_request(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "friend_request_response", "success": False, "error": "Non autenticato"}
        
        friend_id = data.get('friend_id')
        if not friend_id or friend_id == user_id:
            return {"type": "friend_request_response", "success": False, "error": "ID amico non valido"}
        
        async with aiosqlite.connect(self.db_path) as db:
            # Check if already friends or request exists
            cursor = await db.execute(
                "SELECT status FROM friends WHERE (user_id = ? AND friend_id = ?) OR (user_id = ? AND friend_id = ?)",
                (user_id, friend_id, friend_id, user_id)
            )
            existing = await cursor.fetchone()
            
            if existing:
                if existing[0] == 'accepted':
                    return {"type": "friend_request_response", "success": False, "error": "Già amici"}
                else:
                    return {"type": "friend_request_response", "success": False, "error": "Richiesta già inviata"}
            
            # Send request
            await db.execute(
                "INSERT INTO friends (user_id, friend_id, status) VALUES (?, ?, 'pending')",
                (user_id, friend_id)
            )
            await db.commit()
            
            # Notify friend if connected
            if friend_id in self.user_connections:
                friend_ws = self.user_connections[friend_id]
                try:
                    await friend_ws.send(json.dumps({
                        "type": "notification",
                        "title": "Nuova richiesta di amicizia",
                        "message": "Hai ricevuto una richiesta di amicizia!",
                        "notification_type": "friend_request"
                    }))
                except:
                    pass
            
            return {"type": "friend_request_response", "success": True, "message": "Richiesta inviata!"}
    
    async def handle_accept_friend_request(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "accept_friend_response", "success": False, "error": "Non autenticato"}
        
        friend_id = data.get('friend_id')
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE friends SET status = 'accepted' WHERE user_id = ? AND friend_id = ?",
                (friend_id, user_id)
            )
            await db.commit()
            
            return {"type": "accept_friend_response", "success": True, "message": "Amicizia accettata!"}
    
    async def handle_get_friends(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "friends_list", "success": False, "error": "Non autenticato"}
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Get accepted friends
            cursor = await db.execute(
                """SELECT u.id, u.username, u.level, f.created_at as friends_since
                   FROM friends f
                   JOIN users u ON (
                       CASE WHEN f.user_id = ? THEN f.friend_id ELSE f.user_id END = u.id
                   )
                   WHERE (f.user_id = ? OR f.friend_id = ?) AND f.status = 'accepted'""",
                (user_id, user_id, user_id)
            )
            friends = await cursor.fetchall()
            
            # Get pending requests (received)
            cursor = await db.execute(
                """SELECT u.id, u.username, u.level, f.created_at
                   FROM friends f
                   JOIN users u ON f.user_id = u.id
                   WHERE f.friend_id = ? AND f.status = 'pending'""",
                (user_id,)
            )
            pending = await cursor.fetchall()
            
            return {
                "type": "friends_list",
                "success": True,
                "friends": [dict(f) for f in friends],
                "pending_requests": [dict(p) for p in pending]
            }
    
    async def handle_get_cash_tables(self, ws, data: dict):
        tables_info = []
        for table_id, table in self.tables.items():
            if not table.is_private:
                tables_info.append({
                    "table_id": table_id,
                    "name": table.name,
                    "small_blind": table.small_blind,
                    "big_blind": table.big_blind,
                    "min_buy_in": table.min_buy_in,
                    "max_buy_in": table.max_buy_in,
                    "players": len(table.players),
                    "max_players": table.max_players
                })
        
        return {
            "type": "cash_tables_response",
            "success": True,
            "tables": tables_info
        }
    
    async def handle_join_cash_table(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "join_table_response", "success": False, "error": "Non autenticato"}
        
        table_id = data.get('table_id')
        buy_in = data.get('buy_in', 0)
        
        if table_id not in self.tables:
            return {"type": "join_table_response", "success": False, "error": "Tavolo non trovato"}
        
        table = self.tables[table_id]
        
        if buy_in < table.min_buy_in or buy_in > table.max_buy_in:
            return {"type": "join_table_response", "success": False, 
                    "error": f"Buy-in deve essere tra €{table.min_buy_in:.2f} e €{table.max_buy_in:.2f}"}
        
        # Check wallet balance
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT balance FROM wallets WHERE user_id = ?", (user_id,))
            wallet = await cursor.fetchone()
            
            if not wallet or wallet['balance'] < buy_in:
                return {"type": "join_table_response", "success": False, "error": "Saldo insufficiente"}
            
            cursor = await db.execute("SELECT username FROM users WHERE id = ?", (user_id,))
            user = await cursor.fetchone()
            
            # Deduct buy-in from wallet
            await db.execute("UPDATE wallets SET balance = balance - ? WHERE user_id = ?", (buy_in, user_id))
            
            # Record transaction
            await db.execute(
                """INSERT INTO transactions (user_id, type, amount, status, description)
                   VALUES (?, 'table_buy_in', ?, 'completed', ?)""",
                (user_id, -buy_in, f"Buy-in: {table.name}")
            )
            await db.commit()
            
            # Add player to table
            success, result = table.add_player(user_id, user['username'], buy_in)
            
            if success:
                self.user_tables[user_id] = table_id
                
                # Notify all players at table
                await self.broadcast_table_state(table_id)
                
                return {
                    "type": "cash_table_joined",
                    "success": True,
                    "table_id": table_id,
                    "table_name": table.name,
                    "position": result,
                    "chips": buy_in,
                    "game_type": "cash",
                    "table_state": table.get_state(user_id)
                }
            else:
                # Refund if couldn't join
                await db.execute("UPDATE wallets SET balance = balance + ? WHERE user_id = ?", (buy_in, user_id))
                await db.commit()
                return {"type": "join_table_response", "success": False, "error": result}
    
    async def handle_create_private_game(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "friend_game_created", "success": False, "error": "Non autenticato"}
        
        game_name = data.get('game_name', '').strip()
        # Fallback for client using 'name' instead of 'game_name'
        if not game_name:
            game_name = data.get('name', '').strip()
            
        password = data.get('password', '').strip()
        small_blind = float(data.get('small_blind', 0.10))
        big_blind = float(data.get('big_blind', 0.20))
        min_buy_in = float(data.get('min_buy_in', 5.0))
        max_buy_in = float(data.get('max_buy_in', 50.0))
        max_players = int(data.get('max_players', 6))
        
        if not game_name or len(game_name) < 3:
            return {"type": "friend_game_created", "success": False, "error": "Nome partita deve avere almeno 3 caratteri"}
        if not password or len(password) < 4:
            return {"type": "friend_game_created", "success": False, "error": "Password deve avere almeno 4 caratteri"}
        
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """INSERT INTO private_games (creator_id, game_name, password, small_blind, big_blind, min_buy_in, max_buy_in, max_players)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (user_id, game_name, password, small_blind, big_blind, min_buy_in, max_buy_in, max_players)
            )
            game_id = cursor.lastrowid
            await db.commit()
            
            # Create actual poker table
            table_id = f"private_{game_id}"
            table = PokerTable(table_id, game_name, small_blind, big_blind, min_buy_in, max_buy_in, max_players)
            table.is_private = True
            table.password = password
            self.tables[table_id] = table
            
            return {
                "type": "friend_game_created",
                "success": True,
                "game_id": game_id,
                "table_id": table_id,
                "game_name": game_name,
                "message": "Partita privata creata!"
            }
    
    async def handle_join_private_game(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "friend_game_joined", "success": False, "error": "Non autenticato"}
        
        game_name = data.get('game_name', '').strip()
        password = data.get('password', '').strip()
        buy_in = float(data.get('buy_in', 10.0))
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT id, small_blind, big_blind, min_buy_in, max_buy_in, max_players FROM private_games WHERE game_name = ? AND password = ?",
                (game_name, password)
            )
            game = await cursor.fetchone()
            
            if not game:
                return {"type": "friend_game_joined", "success": False, "error": "Nome o password non corretti"}
            
            table_id = f"private_{game['id']}"
            
            # Create table if doesn't exist
            if table_id not in self.tables:
                table = PokerTable(
                    table_id, game_name, 
                    game['small_blind'], game['big_blind'],
                    game['min_buy_in'], game['max_buy_in'],
                    game['max_players']
                )
                table.is_private = True
                table.password = password
                self.tables[table_id] = table
            
            table = self.tables[table_id]
            
            if buy_in < table.min_buy_in or buy_in > table.max_buy_in:
                return {"type": "friend_game_joined", "success": False,
                        "error": f"Buy-in deve essere tra €{table.min_buy_in:.2f} e €{table.max_buy_in:.2f}"}
            
            # Check wallet
            cursor = await db.execute("SELECT balance FROM wallets WHERE user_id = ?", (user_id,))
            wallet = await cursor.fetchone()
            
            if not wallet or wallet['balance'] < buy_in:
                return {"type": "friend_game_joined", "success": False, "error": "Saldo insufficiente"}
            
            cursor = await db.execute("SELECT username FROM users WHERE id = ?", (user_id,))
            user = await cursor.fetchone()
            
            # Deduct buy-in
            await db.execute("UPDATE wallets SET balance = balance - ? WHERE user_id = ?", (buy_in, user_id))
            await db.execute(
                """INSERT INTO transactions (user_id, type, amount, status, description)
                   VALUES (?, 'table_buy_in', ?, 'completed', ?)""",
                (user_id, -buy_in, f"Buy-in: {game_name}")
            )
            await db.commit()
            
            # Add to table
            success, result = table.add_player(user_id, user['username'], buy_in)
            
            if success:
                self.user_tables[user_id] = table_id
                await self.broadcast_table_state(table_id)
                
                return {
                    "type": "friend_game_joined",
                    "success": True,
                    "table_id": table_id,
                    "table_name": game_name,
                    "position": result,
                    "chips": buy_in,
                    "game_type": "private",
                    "table_state": table.get_state(user_id)
                }
            else:
                # Refund
                await db.execute("UPDATE wallets SET balance = balance + ? WHERE user_id = ?", (buy_in, user_id))
                await db.commit()
                return {"type": "friend_game_joined", "success": False, "error": result}
    
    async def handle_leave_table(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "leave_table_response", "success": False, "error": "Non autenticato"}
        
        table_id = self.user_tables.get(user_id)
        if not table_id or table_id not in self.tables:
            return {"type": "leave_table_response", "success": False, "error": "Non sei a un tavolo"}
        
        table = self.tables[table_id]
        remaining_chips = table.remove_player(user_id)
        del self.user_tables[user_id]
        
        # Return chips to wallet
        if remaining_chips > 0:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("UPDATE wallets SET balance = balance + ? WHERE user_id = ?", (remaining_chips, user_id))
                await db.execute(
                    """INSERT INTO transactions (user_id, type, amount, status, description)
                       VALUES (?, 'table_cash_out', ?, 'completed', ?)""",
                    (user_id, remaining_chips, f"Cash out: {table.name}")
                )
                await db.commit()
        
        await self.broadcast_table_state(table_id)
        
        return {
            "type": "leave_table_response",
            "success": True,
            "chips_returned": remaining_chips,
            "message": f"Hai lasciato il tavolo. €{remaining_chips:.2f} restituiti al wallet."
        }
    
    async def handle_get_table_state(self, ws, data: dict):
        user_id = self.connections.get(ws)
        table_id = data.get('table_id') or self.user_tables.get(user_id)
        
        if not table_id or table_id not in self.tables:
            return {"type": "table_state_response", "success": False, "error": "Tavolo non trovato"}
        
        table = self.tables[table_id]
        return {
            "type": "table_state_response",
            "success": True,
            "table_state": table.get_state(user_id)
        }
    
    async def broadcast_table_state(self, table_id: str):
        if table_id not in self.tables:
            return
        
        table = self.tables[table_id]
        for player_id in table.players:
            if player_id in self.user_connections:
                ws = self.user_connections[player_id]
                try:
                    await ws.send(json.dumps({
                        "type": "table_update",
                        "table_state": table.get_state(player_id)
                    }))
                except:
                    pass
    
    async def handle_get_transaction_history(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "transactions_data", "success": False, "error": "Non autenticato"}
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT type, amount, status, description, created_at 
                   FROM transactions WHERE user_id = ? 
                   ORDER BY created_at DESC LIMIT 50""",
                (user_id,)
            )
            transactions = await cursor.fetchall()
            
            return {
                "type": "transactions_data",
                "success": True,
                "transactions": [dict(t) for t in transactions]
            }

    async def handle_get_friend_games(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "friend_games_list", "success": False, "error": "Non autenticato"}
        
        # 1. Get friends list
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT CASE WHEN user_id = ? THEN friend_id ELSE user_id END as fid
                   FROM friends 
                   WHERE (user_id = ? OR friend_id = ?) AND status = 'accepted'""",
                (user_id, user_id, user_id)
            )
            rows = await cursor.fetchall()
            friend_ids = [row['fid'] for row in rows]
            
        friend_games = []
        
        # 2. Check if friends are playing
        for fid in friend_ids:
            if fid in self.user_tables:
                table_id = self.user_tables[fid]
                if table_id in self.tables:
                    table = self.tables[table_id]
                    # Get friend username
                    friend_name = "Unknown"
                    if fid in table.players:
                        friend_name = table.players[fid]['username']
                    
                    friend_games.append({
                        "friend_username": friend_name,
                        "table_name": table.name,
                        "game_type": "Private" if table.is_private else "Cash Game",
                        "blinds": f"€{table.small_blind:.2f}/€{table.big_blind:.2f}",
                        "players": f"{len(table.players)}/{table.max_players}",
                        "table_id": table_id
                    })
        
        return {
            "type": "friend_games_list",
            "success": True,
            "games": friend_games
        }

    async def handle_get_game_history(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "history_data", "success": False, "error": "Non autenticato"}
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT id, game_type, result, chips_change, hand as details, created_at as played_at 
                   FROM game_history WHERE user_id = ? 
                   ORDER BY created_at DESC LIMIT 50""",
                (user_id,)
            )
            history = await cursor.fetchall()
            
            return {
                "type": "history_data",
                "success": True,
                "history": [dict(h) for h in history]
            }

    async def handle_chat_message(self, ws, data: dict):
        user_id = self.connections.get(ws)
        table_id = data.get('table_id')
        message = data.get('message', '').strip()
        
        if not user_id or not table_id or not message:
            return {"type": "chat_sent", "success": False}
            
        # Get username
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT username FROM users WHERE id = ?", (user_id,))
            row = await cursor.fetchone()
            username = row[0] if row else "Unknown"

        # Broadcast to table
        if table_id in self.tables:
            table = self.tables[table_id]
            # Send to players and spectators (if any)
            # For now just players in self.players dict
            targets = list(table.players.keys())
            
            for pid in targets:
                if pid in self.user_connections:
                    pws = self.user_connections[pid]
                    try:
                        await pws.send(json.dumps({
                            "type": "chat_message",
                            "table_id": table_id,
                            "user_id": user_id,
                            "username": username,
                            "message": message
                        }))
                    except:
                        pass
        
        return {"type": "chat_sent", "success": True}

    async def handle_get_leaderboard(self, ws, data: dict):
        leaderboard_type = data.get('leaderboard_type', 'chips') # chips, winnings
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if leaderboard_type == 'winnings':
                cursor = await db.execute("""
                    SELECT u.username, s.total_chips_won as score, u.level
                    FROM statistics s
                    JOIN users u ON s.user_id = u.id
                    ORDER BY s.total_chips_won DESC LIMIT 20
                """)
            else: # chips
                cursor = await db.execute("""
                    SELECT u.username, w.balance as score, u.level
                    FROM wallets w
                    JOIN users u ON w.user_id = u.id
                    ORDER BY w.balance DESC LIMIT 20
                """)
            
            leaders = await cursor.fetchall()
            return {
                "type": "leaderboard_data",
                "success": True,
                "leaderboard": [dict(l) for l in leaders],
                "leaderboard_type": leaderboard_type
            }

    async def handle_update_avatar(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "avatar_update_result", "success": False, "error": "Non autenticato"}
        
        avatar_id = data.get('avatar_id')
        if not isinstance(avatar_id, int) or avatar_id < 0 or avatar_id > 20: # Assuming max 20 avatars
            return {"type": "avatar_update_result", "success": False, "error": "Avatar non valido"}
            
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET avatar_id = ? WHERE id = ?", (avatar_id, user_id))
            await db.commit()
            
            return {
                "type": "avatar_update_result",
                "success": True,
                "avatar_id": avatar_id,
                "message": "Avatar aggiornato!"
            }

    async def handle_message(self, ws, message: str):
        try:
            data = json.loads(message)
            action = data.get('action') or data.get('type', '')
            
            handlers = {
                'ping': self.handle_ping,
                'register': self.handle_register,
                'login': self.handle_login,
                'get_security_question': self.handle_get_security_question,
                'verify_security_answer': self.handle_verify_security_answer,
                'reset_password': self.handle_reset_password,
                'get_wallet': self.handle_get_wallet,
                'create_deposit': self.handle_create_deposit,
                'wallet_deposit': self.handle_create_deposit, # Alias for client
                'verify_deposit': self.handle_verify_deposit,
                'capture_deposit': self.handle_verify_deposit, # Alias for client
                'withdraw': self.handle_withdraw,
                'wallet_withdraw': self.handle_withdraw, # Alias for client
                'get_statistics': self.handle_get_statistics,
                'search_users': self.handle_search_users,
                'send_friend_request': self.handle_send_friend_request,
                'accept_friend_request': self.handle_accept_friend_request,
                'get_friends': self.handle_get_friends,
                'get_cash_tables': self.handle_get_cash_tables,
                'join_cash_table': self.handle_join_cash_table,
                'create_private_game': self.handle_create_private_game,
                'create_friend_game': self.handle_create_private_game, # Alias for client
                'join_private_game': self.handle_join_private_game,
                'join_friend_game': self.handle_join_private_game, # Alias for client
                'leave_table': self.handle_leave_table,
                'get_table_state': self.handle_get_table_state,
                'get_game_history': self.handle_get_game_history,
                'get_transaction_history': self.handle_get_transaction_history,
                'get_friend_games': self.handle_get_friend_games,
                'chat_message': self.handle_chat_message,
                'get_leaderboard': self.handle_get_leaderboard,
                'update_avatar': self.handle_update_avatar,
            }
            
            handler = handlers.get(action)
            if handler:
                response = await handler(ws, data)
                await ws.send(json.dumps(response))
            else:
                await ws.send(json.dumps({
                    "type": "error",
                    "error": f"Unknown action: {action}"
                }))
        except json.JSONDecodeError:
            await ws.send(json.dumps({"type": "error", "error": "Invalid JSON"}))
        except Exception as e:
            print(f"Error handling message: {e}")
            await ws.send(json.dumps({"type": "error", "error": str(e)}))
    
    async def handle_connection(self, ws):
        print(f"New connection from {ws.remote_address}")
        # Send connected acknowledgment immediately
        try:
            await ws.send(json.dumps({"type": "connected", "status": "ok"}))
            print(f"Sent connected ack to {ws.remote_address}")
        except Exception as e:
            print(f"Failed to send connected ack: {e}")
            return
        try:
            async for message in ws:
                await self.handle_message(ws, message)
        except ConnectionClosed:
            pass
        finally:
            # Cleanup
            user_id = self.connections.pop(ws, None)
            if user_id:
                self.user_connections.pop(user_id, None)
                # Handle leaving table on disconnect
                table_id = self.user_tables.get(user_id)
                if table_id and table_id in self.tables:
                    # Keep player at table but mark as disconnected
                    # They can reconnect
                    pass
            print(f"Connection closed: {ws.remote_address}")
    
    async def run(self, host: str = "0.0.0.0", port: int = None):
        port = port or int(os.environ.get("PORT", 8765))
        await self.init_db()
        print(f"Poker Server v13 starting on {host}:{port}")
        print(f"Password recovery: ENABLED")
        print(f"PayPal: {'Configured' if PAYPAL_CLIENT_ID else 'Not configured'}")
        async with websockets.serve(self.handle_connection, host, port):
            await asyncio.Future()

if __name__ == "__main__":
    server = PokerServer()
    asyncio.run(server.run())
