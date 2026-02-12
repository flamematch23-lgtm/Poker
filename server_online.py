#!/usr/bin/env python3
"""
Poker Texas Hold'em Server v14 - Admin Power Update
- Manual Withdrawal Approval
- Server Configuration (Timer, Maintenance)
- Advanced Analytics
- User Inspection
"""

import asyncio
import json
import hashlib
import os
import time
import random
from datetime import datetime, timedelta
import aiohttp
from aiohttp import web
import aiosqlite
import websockets
from websockets.exceptions import ConnectionClosed

# ==========================================
# CONFIGURATION
# ==========================================

SERVER_CONFIG = {
    "maintenance_mode": False,
    "turn_timer": 30, # seconds
    "rake_percentage": 0.0 # Future use
}

CONFIG_FILE = "server_config.json"

def load_config():
    global SERVER_CONFIG
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                SERVER_CONFIG.update(json.load(f))
        except:
            pass

def save_config():
    with open(CONFIG_FILE, "w") as f:
        json.dump(SERVER_CONFIG, f)

load_config()

# ==========================================
# POKER ENGINE
# ==========================================

class Card:
    SUITS = ['s', 'h', 'd', 'c'] # spades, hearts, diamonds, clubs
    RANKS = {2: '2', 3: '3', 4: '4', 5: '5', 6: '6', 7: '7', 8: '8', 9: '9', 10: 'T', 11: 'J', 12: 'Q', 13: 'K', 14: 'A'}
    
    def __init__(self, rank, suit):
        self.rank = rank
        self.suit = suit
        
    def __repr__(self):
        return f"{self.RANKS[self.rank]}{self.suit}"
        
    def to_dict(self):
        return {"rank": self.RANKS[self.rank], "suit": self.suit, "value": self.rank}

class Deck:
    def __init__(self):
        self.cards = [Card(r, s) for r in range(2, 15) for s in Card.SUITS]
        self.shuffle()
        
    def shuffle(self):
        random.shuffle(self.cards)
        
    def deal(self, n=1):
        return [self.cards.pop() for _ in range(n)]

class HandEvaluator:
    @staticmethod
    def evaluate(hole_cards, community_cards):
        cards = hole_cards + community_cards
        if not cards:
            return (0, "No Hand")
            
        # Sort by rank desc
        cards.sort(key=lambda c: c.rank, reverse=True)
        
        # Base Multipliers (using 10 Billion steps to avoid overlap with internal scores)
        # Max internal score (High Card) is roughly 1.5 Billion (14 * 100^4)
        BASE = 10_000_000_000
        
        # Check Flush
        suits = {}
        for c in cards:
            suits[c.suit] = suits.get(c.suit, []) + [c]
        
        flush_suit = None
        flush_cards = []
        for s, suit_cards in suits.items():
            if len(suit_cards) >= 5:
                flush_suit = s
                flush_cards = suit_cards[:5]
                break
                
        # Check Straight
        unique_ranks = sorted(list(set(c.rank for c in cards)), reverse=True)
        straight_ranks = []
        for i in range(len(unique_ranks) - 4):
            window = unique_ranks[i:i+5]
            if window[0] - window[4] == 4:
                straight_ranks = window
                break
        # Wheel straight (A, 2, 3, 4, 5)
        if not straight_ranks and 14 in unique_ranks and 2 in unique_ranks and 3 in unique_ranks and 4 in unique_ranks and 5 in unique_ranks:
            straight_ranks = [5, 4, 3, 2, 14] # 5-high straight
            
        # Check Straight Flush
        sf_ranks = []
        if flush_suit:
            sf_cards_all = [c for c in cards if c.suit == flush_suit]
            sf_unique_ranks = sorted(list(set(c.rank for c in sf_cards_all)), reverse=True)
            for i in range(len(sf_unique_ranks) - 4):
                window = sf_unique_ranks[i:i+5]
                if window[0] - window[4] == 4:
                    sf_ranks = window
                    break
            if not sf_ranks and 14 in sf_unique_ranks and 2 in sf_unique_ranks and 3 in sf_unique_ranks and 4 in sf_unique_ranks and 5 in sf_unique_ranks:
                sf_ranks = [5, 4, 3, 2, 14]

        # 1. Royal Flush
        if sf_ranks and sf_ranks[0] == 14 and sf_ranks[-1] != 14: # Normal Ace High SF (avoids wheel check confusion though A is 14)
             return (9 * BASE, "Royal Flush")
             
        # 2. Straight Flush
        if sf_ranks:
            return (8 * BASE + sf_ranks[0] * 1000000, "Straight Flush")
        
        # Count ranks
        rank_counts = {}
        for c in cards:
            rank_counts[c.rank] = rank_counts.get(c.rank, 0) + 1
            
        pairs = []
        three_kind = []
        four_kind = []
        
        for r, count in rank_counts.items():
            if count == 4: four_kind.append(r)
            elif count == 3: three_kind.append(r)
            elif count == 2: pairs.append(r)
            
        four_kind.sort(reverse=True)
        three_kind.sort(reverse=True)
        pairs.sort(reverse=True)
        
        # 3. Four of a Kind
        if four_kind:
            kicker = max([r for r in unique_ranks if r != four_kind[0]])
            return (7 * BASE + four_kind[0] * 100**4 + kicker * 100**3, "Four of a Kind")
            
        # 4. Full House
        if three_kind and (len(three_kind) >= 2 or pairs):
            t = three_kind[0]
            p = three_kind[1] if len(three_kind) >= 2 else pairs[0]
            return (6 * BASE + t * 100**4 + p * 100**3, "Full House")
            
        # 5. Flush
        if flush_suit:
            score = 5 * BASE
            for i, c in enumerate(flush_cards):
                score += c.rank * (100 ** (4-i))
            return (score, "Flush")
            
        # 6. Straight
        if straight_ranks:
            return (4 * BASE + straight_ranks[0] * 100**4, "Straight")
            
        # 7. Three of a Kind
        if three_kind:
            kickers = sorted([r for r in unique_ranks if r != three_kind[0]], reverse=True)[:2]
            return (3 * BASE + three_kind[0] * 100**4 + kickers[0] * 100**3 + kickers[1] * 100**2, "Three of a Kind")
            
        # 8. Two Pair
        if len(pairs) >= 2:
            p1 = pairs[0]
            p2 = pairs[1]
            kicker = max([r for r in unique_ranks if r != p1 and r != p2])
            return (2 * BASE + p1 * 100**4 + p2 * 100**3 + kicker * 100**2, "Two Pair")
            
        # 9. One Pair
        if pairs:
            kickers = sorted([r for r in unique_ranks if r != pairs[0]], reverse=True)[:3]
            score = 1 * BASE + pairs[0] * 100**4
            for i, k in enumerate(kickers):
                score += k * (100 ** (3-i))
            return (score, "Pair")
            
        # 10. High Card
        score = 0
        for i, c in enumerate(cards[:5]):
            score += c.rank * (100 ** (4-i))
        return (score, "High Card")

# PayPal Configuration
PAYPAL_CLIENT_ID = os.environ.get('PAYPAL_CLIENT_ID', 'ATGUiTFJ0G6kKrJ4RYJ0sg80pZ3qlTqK8WFkIieVu2fU0X354vLFsyel8QVKleajel1ZpgslVsliuVAI')
PAYPAL_SECRET = os.environ.get('PAYPAL_SECRET', 'EPsoCGBkuF3LI8KQKbTWBDhjw6f4gc2RUscrAw9W3baDJlU-0ZyKnuU6qVmAnGbzmn12AcMNcbRRYGgB')
# PAYPAL_API_BASE = "https://api-m.sandbox.paypal.com" # SANDBOX
PAYPAL_API_BASE = "https://api-m.paypal.com" # LIVE

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
                "return_url": "pokertexas://paypal/return",
                "cancel_url": "pokertexas://paypal/cancel"
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
                 min_buy_in: float, max_buy_in: float, max_players: int = 6, creator_id: int = None, creator_username: str = "Unknown"):
        self.table_id = table_id
        self.name = name
        self.small_blind = small_blind
        self.big_blind = big_blind
        self.min_buy_in = min_buy_in
        self.max_buy_in = max_buy_in
        self.max_players = max_players
        self.creator_id = creator_id
        self.creator_username = creator_username
        self.players = {}  # user_id -> {username, chips, position, is_active, is_sitting_out, ...}
        self.spectators = set()
        self.dealer_position = 0
        self.current_player = None # user_id
        self.pot = 0.0
        self.community_cards = []
        self.game_phase = "waiting"  # waiting, preflop, flop, turn, river, showdown
        self.current_bet = 0.0
        self.last_action_time = None
        self.is_private = False
        self.password = None
        
        # Game State
        self.deck = Deck()
        self.winners = []
        self.hand_result = ""
        self.round_bets = {} # user_id -> amount bet in current street
        self.active_seat_order = [] # list of user_ids in seat order for current hand
        self.players_acted = set() # user_ids who acted in current round

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
            'current_bet': 0.0,
            'folded': False,
            'all_in': False
        }
        
        # Try to start game if enough players
        if self.game_phase == "waiting" and len(self.players) >= 2:
            self.start_hand()
            
        return True, position
    
    def remove_player(self, user_id: int):
        if user_id in self.players:
            chips = self.players[user_id]['chips']
            del self.players[user_id]
            
            # If game in progress, handle fold
            if self.game_phase != "waiting" and self.current_player == user_id:
                self.handle_action(user_id, "fold")
            
            # Check if game should end
            active_players = [p for p in self.players.values() if not p['is_sitting_out']]
            if len(active_players) < 2:
                self.game_phase = "waiting"
                self.pot = 0
                self.community_cards = []
                
            return chips
        return 0

    def start_hand(self):
        active_players = [uid for uid, p in self.players.items() if not p['is_sitting_out'] and p['chips'] > 0]
        if len(active_players) < 2:
            self.game_phase = "waiting"
            return

        # Sort by position
        active_players.sort(key=lambda uid: self.players[uid]['position'])
        self.active_seat_order = active_players
        
        # Move Dealer Button
        # Find next dealer index
        current_dealer_idx = -1
        for i, uid in enumerate(self.active_seat_order):
            if self.players[uid]['position'] >= self.dealer_position: # Simple logic, can be improved
                current_dealer_idx = i
                break
        
        dealer_idx = (current_dealer_idx + 1) % len(self.active_seat_order)
        self.dealer_position = self.players[self.active_seat_order[dealer_idx]]['position']
        
        # Reset State
        self.game_phase = "preflop"
        self.pot = 0.0
        self.community_cards = []
        self.deck = Deck()
        self.winners = []
        self.hand_result = ""
        self.round_bets = {uid: 0.0 for uid in active_players}
        self.players_acted = set()
        
        for uid in active_players:
            self.players[uid]['cards'] = self.deck.deal(2)
            self.players[uid]['current_bet'] = 0.0
            self.players[uid]['folded'] = False
            self.players[uid]['all_in'] = False
            
        # Post Blinds
        sb_idx = (dealer_idx + 1) % len(active_players)
        bb_idx = (dealer_idx + 2) % len(active_players)
        
        # Heads up exception (Dealer is SB)
        if len(active_players) == 2:
            sb_idx = dealer_idx
            bb_idx = (dealer_idx + 1) % 2

        sb_user = active_players[sb_idx]
        bb_user = active_players[bb_idx]
        
        self._post_blind(sb_user, self.small_blind)
        self._post_blind(bb_user, self.big_blind)
        
        self.current_bet = self.big_blind
        
        # First to act
        next_idx = (bb_idx + 1) % len(active_players)
        self.current_player = active_players[next_idx]
        
    def _post_blind(self, user_id, amount):
        player = self.players[user_id]
        bet = min(player['chips'], amount)
        player['chips'] -= bet
        player['current_bet'] = bet
        self.pot += bet
        self.round_bets[user_id] = bet
        if player['chips'] == 0:
            player['all_in'] = True

    def handle_action(self, user_id: int, action: str, amount: float = 0):
        player = self.players[user_id]

        if action == "sitout":
            player['is_sitting_out'] = True
            # If in active hand, fold
            if self.game_phase != "waiting" and self.game_phase != "showdown" and not player['folded']:
                 self.handle_action(user_id, "fold")
            return True, "Sitting out"
            
        if action == "sitin":
            player['is_sitting_out'] = False
            return True, "Sitting in"

        if self.game_phase == "waiting" or self.game_phase == "showdown":
            return False, "Game not active"
            
        if user_id != self.current_player:
            return False, "Not your turn"
        
        
        if action == "fold":
            player['folded'] = True
            player['cards'] = []
            player['last_action'] = "FOLD"
            
        elif action == "call":
            to_call = self.current_bet - player['current_bet']
            if to_call > player['chips']:
                # All in
                to_call = player['chips']
                player['all_in'] = True
            
            player['chips'] -= to_call
            player['current_bet'] += to_call
            self.pot += to_call
            self.round_bets[user_id] = player['current_bet']
            player['last_action'] = "CALL"
            if player['all_in']: player['last_action'] = "ALL-IN"
            
        elif action == "check":
            if player['current_bet'] < self.current_bet:
                return False, "Cannot check, must call"
            player['last_action'] = "CHECK"
                
        elif action == "raise":
            if amount < self.current_bet * 2: # Min raise
                 # Allow all-in raise if less than min raise
                 if amount != player['chips'] + player['current_bet']:
                     return False, f"Raise too small. Min: {self.current_bet * 2}"
            
            total_bet = amount
            to_add = total_bet - player['current_bet']
            
            if to_add > player['chips']:
                return False, "Not enough chips"
                
            player['chips'] -= to_add
            player['current_bet'] += to_add
            self.pot += to_add
            self.current_bet = total_bet
            self.round_bets[user_id] = player['current_bet']
            player['last_action'] = "RAISE"
            
            if player['chips'] == 0:
                player['all_in'] = True
                player['last_action'] = "ALL-IN"

        # Check if round complete or next player
        self.players_acted.add(user_id)
        self._next_turn()
        return True, "Action accepted"

    def _next_turn(self):
        # Find next player
        active_players = [uid for uid in self.active_seat_order if not self.players[uid]['folded'] and not self.players[uid]['all_in']]
        
        # Check if only one player left (everyone else folded)
        non_folded = [uid for uid in self.active_seat_order if not self.players[uid]['folded']]
        if len(non_folded) == 1:
            self._end_hand_winner(non_folded[0])
            return

        # Find index of current player
        try:
            curr_idx = self.active_seat_order.index(self.current_player)
        except:
            curr_idx = 0
            
        # Try finding next active player
        next_player = None
        for i in range(1, len(self.active_seat_order)):
            idx = (curr_idx + i) % len(self.active_seat_order)
            uid = self.active_seat_order[idx]
            p = self.players[uid]
            if not p['folded'] and not p['all_in']:
                next_player = uid
                break
        
        # Check if round should end
        all_matched = True
        for uid in self.active_seat_order:
            p = self.players[uid]
            if not p['folded'] and not p['all_in'] and p['current_bet'] != self.current_bet:
                all_matched = False
                break
        
        # Round ends if all matched AND everyone active has acted
        # Exception: if only 1 active player (others all-in) -> they don't need to act if matched
        
        all_acted = True
        for uid in active_players:
            if uid not in self.players_acted:
                # If they are all-in, they don't need to act (handled by active_players filter)
                # If they haven't acted, round continues
                all_acted = False
                break
                
        if all_matched and all_acted:
             self._next_phase()
        else:
            self.current_player = next_player

    def _next_phase(self):
        # Reset current bets for next street
        for uid in self.active_seat_order:
            self.players[uid]['current_bet'] = 0.0
            # Keep last_action visible for a bit? Or reset?
            # If we reset, people might miss what happened.
            # But new street starts with no actions.
            # Let's reset it.
            self.players[uid]['last_action'] = ""
            
        self.current_bet = 0.0
        self.players_acted = set()
        
        if self.game_phase == "preflop":
            self.game_phase = "flop"
            self.community_cards = self.deck.deal(3)
        elif self.game_phase == "flop":
            self.game_phase = "turn"
            self.community_cards += self.deck.deal(1)
        elif self.game_phase == "turn":
            self.game_phase = "river"
            self.community_cards += self.deck.deal(1)
        elif self.game_phase == "river":
            self.game_phase = "showdown"
            self._evaluate_showdown()
            return

        # Set first player to act (first active after dealer)
        dealer_idx = -1
        for i, uid in enumerate(self.active_seat_order):
            if self.players[uid]['position'] == self.dealer_position:
                dealer_idx = i
                break
        
        next_player = None
        for i in range(1, len(self.active_seat_order) + 1):
            idx = (dealer_idx + i) % len(self.active_seat_order)
            uid = self.active_seat_order[idx]
            p = self.players[uid]
            if not p['folded'] and not p['all_in']:
                next_player = uid
                break
                
        if next_player:
            self.current_player = next_player
        else:
            # Everyone all-in? Run it out
            while len(self.community_cards) < 5:
                self.community_cards += self.deck.deal(1)
            self.game_phase = "showdown"
            self._evaluate_showdown()

    def _end_hand_winner(self, winner_id):
        # Single winner (everyone else folded)
        self.players[winner_id]['chips'] += self.pot
        self.winners = [{"user_id": winner_id, "amount": self.pot, "hand": "Opponents Folded"}]
        self.game_phase = "showdown"
        # Reset timer would go here
        
    def _evaluate_showdown(self):
        # Evaluate all hands
        results = []
        for uid in self.active_seat_order:
            p = self.players[uid]
            if not p['folded']:
                score, desc = HandEvaluator.evaluate(p['cards'], self.community_cards)
                results.append({"user_id": uid, "score": score, "desc": desc})
        
        # Sort by score desc
        results.sort(key=lambda x: x['score'], reverse=True)
        
        if not results:
            return

        # Find winners (handle splits)
        best_score = results[0]['score']
        winners = [r for r in results if r['score'] == best_score]
        
        # Split pot
        split_amount = self.pot / len(winners)
        for w in winners:
            self.players[w['user_id']]['chips'] += split_amount
            w['amount'] = split_amount
            w['hand'] = w['desc']
            
        self.winners = winners
        self.hand_result = ", ".join([f"{w['desc']}" for w in winners])
    
    def get_state(self, for_user_id: int = None):
        players_state = []
        for uid, p in self.players.items():
            cards = []
            # Show cards if:
            # 1. It's the user themselves
            # 2. It's showdown and player didn't fold
            if for_user_id == uid or (self.game_phase == "showdown" and not p['folded']):
                cards = [c.to_dict() for c in p['cards']]
            elif p['folded']:
                 cards = [] # Folded cards hidden
            else:
                 cards = [{"rank": "?", "suit": "?", "value": 0}, {"rank": "?", "suit": "?", "value": 0}] if p['cards'] else []

            player_state = {
                'user_id': uid,
                'username': p['username'],
                'chips': p['chips'],
                'position': p['position'],
                'is_active': p['is_active'],
                'is_sitting_out': p['is_sitting_out'],
                'current_bet': p['current_bet'],
                'has_cards': len(p['cards']) > 0,
                'cards': cards,
                'folded': p['folded'],
                'all_in': p['all_in'],
                'last_action': p.get('last_action', '')
            }
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
            'community_cards': [c.to_dict() for c in self.community_cards],
            'game_phase': self.game_phase,
            'current_bet': self.current_bet,
            'winners': self.winners
        }

class PokerServer:
    def __init__(self):
        self.connections = {}  # websocket -> user_id
        self.user_connections = {}  # user_id -> websocket
        self.paypal = PayPalClient()
        self.db_path = "poker_database.db"
        self.tables = {}  # table_id -> PokerTable
        self.user_tables = {}  # user_id -> table_id (active table)
        self.table_timers = {} # table_id -> asyncio.Task
        
        # Define default tables configuration
        self.DEFAULT_TABLES = [
            ("table_micro", "Micro Stakes", 0.05, 0.10, 2.0, 10.0),
            ("table_low", "Low Stakes", 0.10, 0.20, 4.0, 20.0),
            ("table_medium", "Medium Stakes", 0.25, 0.50, 10.0, 50.0),
            ("table_high", "High Stakes", 0.50, 1.00, 20.0, 100.0),
            ("table_vip", "VIP Room", 1.00, 2.00, 40.0, 200.0),
        ]
        
        self._init_default_tables()
    
    def _start_turn_timer(self, table_id):
        # Cancel existing
        if table_id in self.table_timers:
            self.table_timers[table_id].cancel()
            del self.table_timers[table_id]
        
        if table_id in self.tables:
            table = self.tables[table_id]
            if table.game_phase not in ["waiting", "showdown"] and table.current_player:
                duration = SERVER_CONFIG.get("turn_timer", 30)
                task = asyncio.create_task(self._turn_timeout_task(table_id, table.current_player, duration))
                self.table_timers[table_id] = task

    async def _turn_timeout_task(self, table_id, player_id, duration):
        try:
            await asyncio.sleep(duration)
            if table_id in self.tables:
                table = self.tables[table_id]
                if table.current_player == player_id and table.game_phase not in ["waiting", "showdown"]:
                    # Force action: Check if possible, else Fold
                    player = table.players.get(player_id)
                    if player:
                        action = "fold"
                        if player['current_bet'] == table.current_bet:
                            action = "check"
                        
                        print(f"Timeout for user {player_id} at table {table_id}. Forcing {action}.")
                        success, msg = table.handle_action(player_id, action)
                        if success:
                            # Set to sit out
                            table.players[player_id]['is_sitting_out'] = True
                            await self.broadcast_table_state(table_id)
                            # Trigger next timer
                            self._start_turn_timer(table_id)
        except asyncio.CancelledError:
            pass

    def _init_default_tables(self):
        # Create default cash game tables with cent-based blinds
        for table_id, name, sb, bb, min_buy, max_buy in self.DEFAULT_TABLES:
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
                    last_login TIMESTAMP,
                    is_banned INTEGER DEFAULT 0
                )
            ''')
            
            # Check for is_banned column (migration)
            try:
                await db.execute("ALTER TABLE users ADD COLUMN is_banned INTEGER DEFAULT 0")
            except:
                pass
            
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
            print("Database initialized with v14 schema")
    
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
        if SERVER_CONFIG.get("maintenance_mode", False):
            return {"type": "login_result", "success": False, "error": "Server in manutenzione. Riprova più tardi."}
            
        email = data.get('email', '').strip().lower()
        password = data.get('password', '')
        
        if not email or not password:
            return {"type": "login_result", "success": False, "error": "Email e password richiesti"}
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT id, username, chips, level, avatar_id, is_banned FROM users WHERE email = ? AND password_hash = ?",
                (email, self.hash_password(password))
            )
            user = await cursor.fetchone()
            
            if not user:
                return {"type": "login_result", "success": False, "error": "Credenziali non valide"}
            
            if user['is_banned']:
                return {"type": "login_result", "success": False, "error": "Account sospeso. Contatta l'amministratore."}
            
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
            
            # Check for active table
            active_table_id = self.user_tables.get(user_id)
            
            return {
                "type": "login_result",
                "success": True,
                "user_id": user_id,
                "username": user['username'],
                "chips": user['chips'],
                "level": level,
                "avatar_id": user['avatar_id'] if 'avatar_id' in user.keys() else 0,
                "wallet_balance": balance,
                "active_table_id": active_table_id,
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

    async def handle_change_password(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "change_password_result", "success": False, "error": "Non autenticato"}
            
        old_password = data.get('old_password', '')
        new_password = data.get('new_password', '')
        
        if not old_password or not new_password:
            return {"type": "change_password_result", "success": False, "error": "Tutti i campi sono richiesti"}
            
        if len(new_password) < 6:
            return {"type": "change_password_result", "success": False, "error": "La password deve avere almeno 6 caratteri"}
            
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT password_hash FROM users WHERE id = ?", (user_id,))
            user = await cursor.fetchone()
            
            if not user:
                return {"type": "change_password_result", "success": False, "error": "Utente non trovato"}
                
            # Verify old password
            if self.hash_password(old_password) != user['password_hash']:
                return {"type": "change_password_result", "success": False, "error": "Vecchia password non corretta"}
                
            # Update to new password
            new_hash = self.hash_password(new_password)
            await db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (new_hash, user_id))
            await db.commit()
            
            return {
                "type": "change_password_result",
                "success": True, 
                "message": "Password modificata con successo"
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
        method = data.get('payment_method', 'paypal')

        if amount < 1:
            return {"type": "wallet_deposit_result", "success": False, "error": "Importo minimo: €1"}
        if amount > 1000:
            return {"type": "wallet_deposit_result", "success": False, "error": "Importo massimo: €1000"}
        
        try:
            order = await self.paypal.create_order(amount, description=f"Deposit via {method.upper()}")
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
                       VALUES (?, 'deposit', ?, 'pending', ?, ?)""",
                    (user_id, amount, order_id, f"Deposit via {method.upper()}")
                )
                await db.commit()
            
            return {
                "type": "wallet_deposit_result",
                "success": True,
                "order_id": order_id,
                "approval_url": approval_url,
                "amount": amount,
                "payment_method": method
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
    
    async def handle_cancel_deposit(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "cancel_deposit_result", "success": False, "error": "Non autenticato"}
        
        order_id = data.get('order_id')
        if not order_id:
            return {"type": "cancel_deposit_result", "success": False, "error": "Order ID mancante"}
            
        async with aiosqlite.connect(self.db_path) as db:
            # Check if transaction exists and is pending
            cursor = await db.execute(
                "SELECT id FROM transactions WHERE paypal_order_id = ? AND user_id = ? AND status = 'pending'",
                (order_id, user_id)
            )
            tx = await cursor.fetchone()
            
            if tx:
                # Update transaction status
                await db.execute(
                    "UPDATE transactions SET status = 'cancelled' WHERE paypal_order_id = ?",
                    (order_id,)
                )
                await db.commit()
                
                return {
                    "type": "cancel_deposit_result",
                    "success": True,
                    "message": "Deposito annullato"
                }
            else:
                return {
                    "type": "cancel_deposit_result",
                    "success": False,
                    "error": "Transazione non trovata o già processata"
                }

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
            
            # Deduct funds immediately to hold them
            await db.execute(
                """UPDATE wallets SET 
                   balance = balance - ?,
                   last_withdrawal = CURRENT_TIMESTAMP
                   WHERE user_id = ?""",
                (amount, user_id)
            )
            
            # Record transaction as pending approval
            await db.execute(
                """INSERT INTO transactions (user_id, type, amount, status, description)
                   VALUES (?, 'withdrawal', ?, 'pending_approval', ?)""",
                (user_id, amount, f"PayPal Payout: {paypal_email}")
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
                "message": f"Richiesta di prelievo di €{amount:.2f} inviata. In attesa di approvazione."
            }
    
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
                self._start_turn_timer(table_id)
                
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
            # Get username
            cursor = await db.execute("SELECT username FROM users WHERE id = ?", (user_id,))
            row = await cursor.fetchone()
            creator_username = row[0] if row else "Unknown"

            cursor = await db.execute(
                """INSERT INTO private_games (creator_id, game_name, password, small_blind, big_blind, min_buy_in, max_buy_in, max_players)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (user_id, game_name, password, small_blind, big_blind, min_buy_in, max_buy_in, max_players)
            )
            game_id = cursor.lastrowid
            await db.commit()
            
            # Create actual poker table
            table_id = f"private_{game_id}"
            table = PokerTable(table_id, game_name, small_blind, big_blind, min_buy_in, max_buy_in, max_players, creator_id=user_id, creator_username=creator_username)
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
                self._start_turn_timer(table_id)
                
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
        
        friend_games = []
        
        # Return ALL private tables (not just friends) so creators can see their own tables
        for table_id, table in self.tables.items():
            if table.is_private:
                friend_games.append({
                    "id": table_id,
                    "name": table.name,
                    "game_type": "Private",
                    "creator": table.creator_username,
                    "creator_id": table.creator_id,
                    "current_players": len(table.players),
                    "max_players": table.max_players,
                    "buy_in": table.min_buy_in,
                    "small_blind": table.small_blind,
                    "big_blind": table.big_blind,
                    "status": table.game_phase,
                    "blinds": f"€{table.small_blind:.2f}/€{table.big_blind:.2f}",
                    "players": f"{len(table.players)}/{table.max_players}",
                    "table_id": table_id
                })
        
        return {
            "type": "friend_games_list",
            "success": True,
            "games": friend_games
        }

    async def handle_delete_friend_game(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "delete_friend_game_result", "success": False, "error": "Non autenticato"}
            
        table_id = data.get('table_id')
        if not table_id or table_id not in self.tables:
            return {"type": "delete_friend_game_result", "success": False, "error": "Tavolo non trovato"}
            
        table = self.tables[table_id]
        
        # Check if creator
        if table.creator_id != user_id:
            return {"type": "delete_friend_game_result", "success": False, "error": "Solo il creatore può eliminare il tavolo"}
            
        # Check if empty
        if len(table.players) > 0:
             return {"type": "delete_friend_game_result", "success": False, "error": "Impossibile eliminare: ci sono giocatori al tavolo"}
             
        # Delete
        del self.tables[table_id]
        
        # Update DB status
        if table_id.startswith("private_"):
            try:
                game_id = int(table_id.split("_")[1])
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute("UPDATE private_games SET status = 'closed' WHERE id = ?", (game_id,))
                    await db.commit()
            except:
                pass
                
        return {"type": "delete_friend_game_result", "success": True, "table_id": table_id}

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
                    SELECT u.username, s.games_won as score, u.level
                    FROM statistics s
                    JOIN users u ON s.user_id = u.id
                    ORDER BY s.games_won DESC LIMIT 20
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
        if not isinstance(avatar_id, int) or avatar_id < 0 or avatar_id > 50: # Increased limit for new avatars
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

    async def handle_game_action(self, ws, data: dict):
        user_id = self.connections.get(ws)
        if not user_id:
            return {"type": "error", "error": "Non autenticato"}
        
        table_id = self.user_tables.get(user_id)
        if not table_id or table_id not in self.tables:
            return {"type": "error", "error": "Non sei a un tavolo"}
        
        table = self.tables[table_id]
        action = data.get('action') # check, call, raise, fold
        amount = float(data.get('amount', 0))
        
        success, message = table.handle_action(user_id, action, amount)
        
        if success:
            # Broadcast update
            await self.broadcast_table_state(table_id)
            
            # If game ended (showdown), wait and restart
            if table.game_phase == "showdown":
                # Notify winners
                await self.broadcast_table_state(table_id)
                # Cancel timer if any
                if table_id in self.table_timers:
                    self.table_timers[table_id].cancel()
                
                # Wait 5 seconds then restart (async sleep in background task would be better but blocking here for simplicity is risky)
                # We should trigger a delayed restart. 
                # For now, let's just leave it at showdown. 
                # Ideally: asyncio.create_task(self.restart_hand(table_id))
                asyncio.create_task(self.restart_hand(table_id))
            else:
                self._start_turn_timer(table_id)
                
            return {"type": "action_result", "success": True}
        else:
            return {"type": "action_result", "success": False, "error": message}

    async def restart_hand(self, table_id):
        await asyncio.sleep(8) # Wait 8 seconds to show results
        if table_id in self.tables:
            table = self.tables[table_id]
            table.start_hand()
            await self.broadcast_table_state(table_id)
            self._start_turn_timer(table_id)

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
                'change_password': self.handle_change_password,
                'get_wallet': self.handle_get_wallet,
                'create_deposit': self.handle_create_deposit,
                'wallet_deposit': self.handle_create_deposit, # Alias for client
                'verify_deposit': self.handle_verify_deposit,
                'capture_deposit': self.handle_verify_deposit, # Alias for client
                'cancel_deposit': self.handle_cancel_deposit,
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
                'check': self.handle_game_action,
                'call': self.handle_game_action,
                'raise': self.handle_game_action,
                'fold': self.handle_game_action,
                'sitout': self.handle_game_action,
                'sitin': self.handle_game_action,
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
                    table = self.tables[table_id]
                    # Mark as sitting out
                    if user_id in table.players:
                        table.players[user_id]['is_sitting_out'] = True
                        
                        # If it was their turn, force fold/check to unblock game
                        if table.current_player == user_id and table.game_phase not in ["waiting", "showdown"]:
                            action = "fold"
                            if table.players[user_id]['current_bet'] == table.current_bet:
                                action = "check"
                            table.handle_action(user_id, action)
                            self._start_turn_timer(table_id)
                            
                        # Broadcast update
                        # We need to run this async, but we are in finally block of an async function
                        # so we can await
                        try:
                            await self.broadcast_table_state(table_id)
                        except:
                            pass
            print(f"Connection closed: {ws.remote_address}")
    
    async def admin_serve_dashboard(self, request):
        try:
            path = "admin_panel.html"
            if not os.path.exists(path):
                path = os.path.join(os.path.dirname(__file__), "admin_panel.html")
                
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            return web.Response(text=content, content_type="text/html")
        except FileNotFoundError:
            return web.Response(text=f"Admin panel file not found. Checked: {path}", status=404)

    async def admin_get_users(self, request):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("""
                SELECT u.id, u.username, u.email, u.chips, u.level, u.is_banned, w.balance as wallet_balance
                FROM users u
                LEFT JOIN wallets w ON u.id = w.user_id
            """)
            rows = await cursor.fetchall()
            
            users = []
            for row in rows:
                u = dict(row)
                u['is_online'] = u['id'] in self.user_connections
                users.append(u)
            
            return web.json_response(users)

    async def admin_get_tables(self, request):
        tables_data = []
        for tid, table in self.tables.items():
            tables_data.append({
                "id": tid,
                "name": table.name,
                "players": len(table.players),
                "max_players": table.max_players,
                "small_blind": table.small_blind,
                "big_blind": table.big_blind,
                "pot": table.pot,
                "phase": table.game_phase
            })
        return web.json_response(tables_data)

    async def admin_update_balance(self, request):
        try:
            user_id = int(request.match_info['id'])
            data = await request.json()
            amount = float(data.get('amount', 0))
            
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("UPDATE wallets SET balance = ? WHERE user_id = ?", (amount, user_id))
                await db.commit()
            
            return web.json_response({"success": True})
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def admin_ban_user(self, request):
        try:
            user_id = int(request.match_info['id'])
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("UPDATE users SET is_banned = 1 WHERE id = ?", (user_id,))
                await db.commit()
            
            # Disconnect if online
            if user_id in self.user_connections:
                ws = self.user_connections[user_id]
                await ws.close()
                
            return web.json_response({"success": True})
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def admin_unban_user(self, request):
        try:
            user_id = int(request.match_info['id'])
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("UPDATE users SET is_banned = 0 WHERE id = ?", (user_id,))
                await db.commit()
            return web.json_response({"success": True})
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def admin_get_transactions(self, request):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("""
                SELECT t.*, u.username 
                FROM transactions t
                JOIN users u ON t.user_id = u.id
                ORDER BY t.created_at DESC LIMIT 100
            """)
            rows = await cursor.fetchall()
            return web.json_response([dict(r) for r in rows])

    async def admin_broadcast_message(self, request):
        try:
            data = await request.json()
            message = data.get('message', '')
            if not message:
                return web.json_response({"success": False, "error": "Message required"}, status=400)
            
            payload = json.dumps({
                "type": "notification",
                "title": "Messaggio di Sistema",
                "message": message,
                "notification_type": "system"
            })
            
            count = 0
            for ws in list(self.connections.keys()):
                try:
                    await ws.send(payload)
                    count += 1
                except:
                    pass
                    
            return web.json_response({"success": True, "count": count})
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def admin_get_global_game_history(self, request):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("""
                SELECT h.id, h.game_type, h.result, h.chips_change, h.hand, h.created_at, u.username
                FROM game_history h
                JOIN users u ON h.user_id = u.id
                ORDER BY h.created_at DESC LIMIT 100
            """)
            rows = await cursor.fetchall()
            return web.json_response([dict(r) for r in rows])

    async def admin_delete_table(self, request):
        try:
            table_id = request.match_info['id']
            if table_id not in self.tables:
                return web.json_response({"success": False, "error": "Table not found"}, status=404)
            
            table = self.tables[table_id]
            
            # Refund everyone
            async with aiosqlite.connect(self.db_path) as db:
                for uid, player in table.players.items():
                    chips = player['chips'] + player['current_bet']
                    
                    if chips > 0:
                        await db.execute("UPDATE wallets SET balance = balance + ? WHERE user_id = ?", (chips, uid))
                        await db.execute(
                            """INSERT INTO transactions (user_id, type, amount, status, description)
                               VALUES (?, 'admin_refund', ?, 'completed', ?)""",
                            (uid, chips, f"Admin closed table: {table.name}")
                        )
                await db.commit()

            # Notify players
            for uid in list(table.players.keys()):
                if uid in self.user_connections:
                    ws = self.user_connections[uid]
                    try:
                        await ws.send(json.dumps({
                            "type": "notification",
                            "title": "Tavolo Chiuso",
                            "message": "Il tavolo è stato chiuso dall'amministratore.",
                            "notification_type": "system"
                        }))
                        # Remove from user_tables
                        if uid in self.user_tables:
                            del self.user_tables[uid]
                    except:
                        pass

            # Delete table
            del self.tables[table_id]
            if table_id in self.table_timers:
                self.table_timers[table_id].cancel()
                del self.table_timers[table_id]
                
            # If private, update DB
            if table_id.startswith("private_"):
                try:
                    game_id = int(table_id.split("_")[1])
                    async with aiosqlite.connect(self.db_path) as db:
                        await db.execute("UPDATE private_games SET status = 'closed_admin' WHERE id = ?", (game_id,))
                        await db.commit()
                except:
                    pass

            return web.json_response({"success": True})
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def admin_restore_defaults(self, request):
        """Restore missing default tables"""
        restored = []
        for table_id, name, sb, bb, min_buy, max_buy in self.DEFAULT_TABLES:
            if table_id not in self.tables:
                self.tables[table_id] = PokerTable(table_id, name, sb, bb, min_buy, max_buy)
                restored.append(name)
        
        return web.json_response({"success": True, "restored": restored})

    async def admin_get_closed_games(self, request):
        """Get closed private games that can be reactivated"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("""
                SELECT g.*, u.username as creator_name
                FROM private_games g
                JOIN users u ON g.creator_id = u.id
                WHERE g.status IN ('closed', 'closed_admin')
                ORDER BY g.created_at DESC LIMIT 50
            """)
            rows = await cursor.fetchall()
            return web.json_response([dict(r) for r in rows])

    async def admin_reactivate_game(self, request):
        """Reactivate a closed private game"""
        try:
            game_id = int(request.match_info['id'])
            
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute("SELECT * FROM private_games WHERE id = ?", (game_id,))
                game = await cursor.fetchone()
                
                if not game:
                    return web.json_response({"success": False, "error": "Game not found"}, status=404)
                
                table_id = f"private_{game_id}"
                if table_id in self.tables:
                    return web.json_response({"success": False, "error": "Table is already active"})
                
                # Get creator username
                cursor = await db.execute("SELECT username FROM users WHERE id = ?", (game['creator_id'],))
                creator = await cursor.fetchone()
                creator_username = creator['username'] if creator else "Unknown"
                
                # Re-create table
                table = PokerTable(
                    table_id, game['game_name'], 
                    game['small_blind'], game['big_blind'],
                    game['min_buy_in'], game['max_buy_in'],
                    game['max_players'],
                    creator_id=game['creator_id'],
                    creator_username=creator_username
                )
                table.is_private = True
                table.password = game['password']
                self.tables[table_id] = table
                
                # Update DB
                await db.execute("UPDATE private_games SET status = 'waiting' WHERE id = ?", (game_id,))
                await db.commit()
                
            return web.json_response({"success": True})
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def admin_update_table(self, request):
        """Update table parameters and restart it"""
        try:
            table_id = request.match_info['id']
            data = await request.json()
            
            sb = float(data.get('small_blind', 0))
            bb = float(data.get('big_blind', 0))
            min_buy = float(data.get('min_buy_in', 0))
            max_buy = float(data.get('max_buy_in', 0))
            
            if sb <= 0 or bb <= 0 or min_buy <= 0 or max_buy <= 0:
                 return web.json_response({"success": False, "error": "Values must be positive"}, status=400)

            # 1. Handle Active Table (Close & Refund first)
            old_name = "Table"
            creator_id = None
            creator_username = "Unknown"
            is_private = False
            password = None
            max_players = 6
            
            if table_id in self.tables:
                table = self.tables[table_id]
                old_name = table.name
                creator_id = table.creator_id
                creator_username = table.creator_username
                is_private = table.is_private
                password = table.password
                max_players = table.max_players
                
                # Refund everyone
                async with aiosqlite.connect(self.db_path) as db:
                    for uid, player in table.players.items():
                        chips = player['chips'] + player['current_bet']
                        if chips > 0:
                            await db.execute("UPDATE wallets SET balance = balance + ? WHERE user_id = ?", (chips, uid))
                            await db.execute(
                                """INSERT INTO transactions (user_id, type, amount, status, description)
                                   VALUES (?, 'admin_refund', ?, 'completed', ?)""",
                                (uid, chips, f"Table updated: {table.name}")
                            )
                    await db.commit()
                
                # Notify
                for uid in list(table.players.keys()):
                    if uid in self.user_connections:
                        try:
                            await self.user_connections[uid].send(json.dumps({
                                "type": "notification",
                                "title": "Tavolo Aggiornato",
                                "message": "Il tavolo è stato riavviato con nuovi parametri.",
                                "notification_type": "system"
                            }))
                            if uid in self.user_tables: del self.user_tables[uid]
                        except: pass
                
                # Remove old instance
                del self.tables[table_id]
                if table_id in self.table_timers:
                    self.table_timers[table_id].cancel()
                    del self.table_timers[table_id]

            # 2. Update Definitions (for persistence in this session)
            # Check if default table
            for i, (tid, name, _, _, _, _) in enumerate(self.DEFAULT_TABLES):
                if tid == table_id:
                    self.DEFAULT_TABLES[i] = (tid, name, sb, bb, min_buy, max_buy)
                    old_name = name # Ensure name is kept
                    break
            
            # Check if private game (Update DB)
            if table_id.startswith("private_"):
                try:
                    game_id = int(table_id.split("_")[1])
                    async with aiosqlite.connect(self.db_path) as db:
                        # Fetch original data if we didn't get it from active table
                        if not creator_id:
                            cursor = await db.execute("SELECT * FROM private_games WHERE id = ?", (game_id,))
                            row = await cursor.fetchone()
                            if row:
                                old_name = row['game_name']
                                creator_id = row['creator_id']
                                password = row['password']
                                max_players = row['max_players']
                                is_private = True
                                
                                # Get creator name
                                cursor = await db.execute("SELECT username FROM users WHERE id = ?", (creator_id,))
                                u_row = await cursor.fetchone()
                                creator_username = u_row['username'] if u_row else "Unknown"

                        await db.execute("""
                            UPDATE private_games 
                            SET small_blind=?, big_blind=?, min_buy_in=?, max_buy_in=?, status='waiting'
                            WHERE id=?
                        """, (sb, bb, min_buy, max_buy, game_id))
                        await db.commit()
                except:
                    pass

            # 3. Create New Instance
            new_table = PokerTable(table_id, old_name, sb, bb, min_buy, max_buy, max_players, creator_id, creator_username)
            if is_private:
                new_table.is_private = True
                new_table.password = password
                
            self.tables[table_id] = new_table
            
            return web.json_response({"success": True})

        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    # --- NEW ADMIN ROUTES ---

    async def admin_get_pending_withdrawals(self, request):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("""
                SELECT t.id, t.amount, t.description, t.created_at, u.username, u.email
                FROM transactions t
                JOIN users u ON t.user_id = u.id
                WHERE t.status = 'pending_approval' AND t.type = 'withdrawal'
                ORDER BY t.created_at DESC
            """)
            rows = await cursor.fetchall()
            return web.json_response([dict(r) for r in rows])

    async def admin_approve_withdrawal(self, request):
        try:
            tx_id = int(request.match_info['id'])
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute("SELECT amount, description FROM transactions WHERE id = ?", (tx_id,))
                tx = await cursor.fetchone()
                
                if not tx:
                    return web.json_response({"success": False, "error": "Transazione non trovata"}, status=404)
                
                amount = tx['amount']
                desc = tx['description'] # Format "PayPal Payout: email"
                email = desc.split(": ")[1].strip() if ": " in desc else ""
                
                if not email:
                     return web.json_response({"success": False, "error": "Email PayPal non trovata nella descrizione"}, status=400)

                # Call PayPal
                try:
                    payout = await self.paypal.create_payout(email, amount)
                    if 'batch_header' in payout:
                        await db.execute("UPDATE transactions SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = ?", (tx_id,))
                        await db.commit()
                        return web.json_response({"success": True})
                    else:
                        return web.json_response({"success": False, "error": "Errore PayPal: " + str(payout)}, status=500)
                except Exception as pp_err:
                     return web.json_response({"success": False, "error": "Eccezione PayPal: " + str(pp_err)}, status=500)
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def admin_reject_withdrawal(self, request):
        try:
            tx_id = int(request.match_info['id'])
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute("SELECT user_id, amount FROM transactions WHERE id = ?", (tx_id,))
                tx = await cursor.fetchone()
                
                if not tx:
                    return web.json_response({"success": False, "error": "Transazione non trovata"}, status=404)
                
                # Refund to wallet
                await db.execute("UPDATE wallets SET balance = balance + ? WHERE user_id = ?", (tx['amount'], tx['user_id']))
                # Mark tx as rejected
                await db.execute("UPDATE transactions SET status = 'rejected', completed_at = CURRENT_TIMESTAMP WHERE id = ?", (tx_id,))
                
                await db.commit()
            return web.json_response({"success": True})
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def admin_get_config(self, request):
        return web.json_response(SERVER_CONFIG)

    async def admin_update_config(self, request):
        try:
            data = await request.json()
            SERVER_CONFIG.update(data)
            save_config()
            return web.json_response({"success": True})
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def admin_get_analytics(self, request):
        async with aiosqlite.connect(self.db_path) as db:
            # Daily stats could be complex, for now simple aggregates
            
            # Total chips
            cursor = await db.execute("SELECT SUM(balance) FROM wallets")
            total_chips = (await cursor.fetchone())[0] or 0
            
            # Total users
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total_users = (await cursor.fetchone())[0] or 0
            
            # Games played today (from game_history)
            today = datetime.now().strftime("%Y-%m-%d")
            cursor = await db.execute("SELECT COUNT(*) FROM game_history WHERE date(created_at) = ?", (today,))
            games_today = (await cursor.fetchone())[0] or 0
            
            return web.json_response({
                "total_chips": total_chips,
                "total_users": total_users,
                "games_today": games_today,
                # Mock data for charts if DB is empty
                "daily_chips": [1000, 1500, 1200, 1800, 2000, 2500, total_chips], 
                "daily_users": [5, 10, 15, 20, 25, 30, total_users]
            })

    async def admin_get_user_details(self, request):
        try:
            user_id = int(request.match_info['id'])
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                
                # Basic info
                cursor = await db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
                user = dict(await cursor.fetchone())
                
                # Stats
                cursor = await db.execute("SELECT * FROM statistics WHERE user_id = ?", (user_id,))
                stats = dict(await cursor.fetchone() or {})
                
                # Recent history
                cursor = await db.execute("SELECT * FROM game_history WHERE user_id = ? ORDER BY created_at DESC LIMIT 20", (user_id,))
                history = [dict(r) for r in await cursor.fetchall()]
                
                # Recent login IPs (mocked for now as we don't store IP yet)
                ips = ["127.0.0.1", "192.168.1.5"]
                
                return web.json_response({
                    "user": user,
                    "stats": stats,
                    "history": history,
                    "ips": ips
                })
        except Exception as e:
             return web.json_response({"success": False, "error": str(e)}, status=500)

    async def run(self, host: str = "0.0.0.0", port: int = None):
        port = port or int(os.environ.get("PORT", 8765))
        admin_port = 8766
        
        await self.init_db()
        print(f"Poker Server v14 starting on {host}:{port}")
        print(f"Password recovery: ENABLED")
        print(f"PayPal: {'Configured' if PAYPAL_CLIENT_ID else 'Not configured'}")
        
        # Start Admin Server
        app = web.Application()
        # CORS
        import aiohttp_cors
        cors = aiohttp_cors.setup(app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
            )
        })
        
        # Routes
        app.router.add_get('/', self.admin_serve_dashboard)
        
        # API Routes with CORS
        # Existing
        resource_users = cors.add(app.router.add_resource("/api/admin/users"))
        cors.add(resource_users.add_route("GET", self.admin_get_users))
        
        resource_tables = cors.add(app.router.add_resource("/api/admin/tables"))
        cors.add(resource_tables.add_route("GET", self.admin_get_tables))
        
        resource_balance = cors.add(app.router.add_resource("/api/admin/users/{id}/balance"))
        cors.add(resource_balance.add_route("POST", self.admin_update_balance))
        
        resource_ban = cors.add(app.router.add_resource("/api/admin/users/{id}/ban"))
        cors.add(resource_ban.add_route("POST", self.admin_ban_user))
        
        resource_unban = cors.add(app.router.add_resource("/api/admin/users/{id}/unban"))
        cors.add(resource_unban.add_route("POST", self.admin_unban_user))
        
        resource_transactions = cors.add(app.router.add_resource("/api/admin/transactions"))
        cors.add(resource_transactions.add_route("GET", self.admin_get_transactions))
        
        resource_broadcast = cors.add(app.router.add_resource("/api/admin/broadcast"))
        cors.add(resource_broadcast.add_route("POST", self.admin_broadcast_message))
        
        resource_game_history = cors.add(app.router.add_resource("/api/admin/game_history"))
        cors.add(resource_game_history.add_route("GET", self.admin_get_global_game_history))
        
        resource_table_delete = cors.add(app.router.add_resource("/api/admin/tables/{id}"))
        cors.add(resource_table_delete.add_route("DELETE", self.admin_delete_table))

        # Table Management
        resource_restore_defaults = cors.add(app.router.add_resource("/api/admin/tables/restore_defaults"))
        cors.add(resource_restore_defaults.add_route("POST", self.admin_restore_defaults))

        resource_closed_games = cors.add(app.router.add_resource("/api/admin/tables/closed"))
        cors.add(resource_closed_games.add_route("GET", self.admin_get_closed_games))

        resource_reactivate_game = cors.add(app.router.add_resource("/api/admin/tables/{id}/reactivate"))
        cors.add(resource_reactivate_game.add_route("POST", self.admin_reactivate_game))

        resource_update_table = cors.add(app.router.add_resource("/api/admin/tables/{id}/update"))
        cors.add(resource_update_table.add_route("POST", self.admin_update_table))

        # NEW ROUTES
        resource_pending_withdrawals = cors.add(app.router.add_resource("/api/admin/withdrawals/pending"))
        cors.add(resource_pending_withdrawals.add_route("GET", self.admin_get_pending_withdrawals))

        resource_approve_withdrawal = cors.add(app.router.add_resource("/api/admin/withdrawals/{id}/approve"))
        cors.add(resource_approve_withdrawal.add_route("POST", self.admin_approve_withdrawal))

        resource_reject_withdrawal = cors.add(app.router.add_resource("/api/admin/withdrawals/{id}/reject"))
        cors.add(resource_reject_withdrawal.add_route("POST", self.admin_reject_withdrawal))

        resource_config = cors.add(app.router.add_resource("/api/admin/config"))
        cors.add(resource_config.add_route("GET", self.admin_get_config))
        cors.add(resource_config.add_route("POST", self.admin_update_config))

        resource_analytics = cors.add(app.router.add_resource("/api/admin/analytics"))
        cors.add(resource_analytics.add_route("GET", self.admin_get_analytics))

        resource_user_details = cors.add(app.router.add_resource("/api/admin/users/{id}/details"))
        cors.add(resource_user_details.add_route("GET", self.admin_get_user_details))
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host, admin_port)
        await site.start()
        print(f"Admin Panel running on http://{host}:{admin_port}")

        async with websockets.serve(self.handle_connection, host, port):
            await asyncio.Future()


if __name__ == "__main__":
    server = PokerServer()
    asyncio.run(server.run())
