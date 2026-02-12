import unittest
from server_online import Card, Deck, HandEvaluator

def C(rank_str, suit_str):
    rank_map = {
        '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9,
        '10': 10, 'T': 10, 'J': 11, 'Q': 12, 'K': 13, 'A': 14
    }
    # Handle suit mapping if needed (test uses full names, Card uses single char)
    # Card.SUITS = ['s', 'h', 'd', 'c']
    suit_map = {
        'hearts': 'h', 'diamonds': 'd', 'clubs': 'c', 'spades': 's',
        'h': 'h', 'd': 'd', 'c': 'c', 's': 's'
    }
    return Card(rank_map[str(rank_str)], suit_map[suit_str])

class TestPokerLogic(unittest.TestCase):
    def test_royal_flush(self):
        hole = [C('A', 'hearts'), C('K', 'hearts')]
        comm = [C('Q', 'hearts'), C('J', 'hearts'), C('10', 'hearts'), C('2', 'clubs'), C('3', 'diamonds')]
        score, name = HandEvaluator.evaluate(hole, comm)
        self.assertGreaterEqual(score, 90_000_000_000)
        self.assertEqual(name, "Royal Flush")

    def test_straight_flush(self):
        hole = [C('9', 'hearts'), C('K', 'hearts')]
        comm = [C('Q', 'hearts'), C('J', 'hearts'), C('10', 'hearts'), C('2', 'clubs'), C('3', 'diamonds')]
        score, name = HandEvaluator.evaluate(hole, comm)
        self.assertGreaterEqual(score, 80_000_000_000)
        self.assertLess(score, 90_000_000_000)
        self.assertEqual(name, "Straight Flush")

    def test_four_of_a_kind(self):
        hole = [C('A', 'hearts'), C('A', 'diamonds')]
        comm = [C('A', 'clubs'), C('A', 'spades'), C('K', 'hearts'), C('2', 'clubs'), C('3', 'diamonds')]
        score, name = HandEvaluator.evaluate(hole, comm)
        self.assertGreaterEqual(score, 70_000_000_000)
        self.assertLess(score, 80_000_000_000)
        self.assertEqual(name, "Four of a Kind")

    def test_full_house(self):
        hole = [C('A', 'hearts'), C('A', 'diamonds')]
        comm = [C('A', 'clubs'), C('K', 'spades'), C('K', 'hearts'), C('2', 'clubs'), C('3', 'diamonds')]
        score, name = HandEvaluator.evaluate(hole, comm)
        self.assertGreaterEqual(score, 60_000_000_000)
        self.assertLess(score, 70_000_000_000)
        self.assertEqual(name, "Full House")

    def test_flush(self):
        hole = [C('A', 'hearts'), C('2', 'hearts')]
        comm = [C('4', 'hearts'), C('6', 'hearts'), C('8', 'hearts'), C('2', 'clubs'), C('3', 'diamonds')]
        score, name = HandEvaluator.evaluate(hole, comm)
        self.assertGreaterEqual(score, 50_000_000_000)
        self.assertLess(score, 60_000_000_000)
        self.assertEqual(name, "Flush")

    def test_straight(self):
        hole = [C('2', 'clubs'), C('3', 'diamonds')]
        comm = [C('4', 'hearts'), C('5', 'spades'), C('6', 'clubs'), C('K', 'hearts'), C('A', 'hearts')]
        score, name = HandEvaluator.evaluate(hole, comm)
        self.assertGreaterEqual(score, 40_000_000_000)
        self.assertLess(score, 50_000_000_000)
        self.assertEqual(name, "Straight")

    def test_straight_ace_low(self):
        hole = [C('A', 'clubs'), C('2', 'diamonds')]
        comm = [C('3', 'hearts'), C('4', 'spades'), C('5', 'clubs'), C('K', 'hearts'), C('Q', 'hearts')]
        score, name = HandEvaluator.evaluate(hole, comm)
        self.assertGreaterEqual(score, 40_000_000_000)
        self.assertLess(score, 50_000_000_000)
        # Note: Ace low straight is A-2-3-4-5. The highest card is 5.
        self.assertEqual(name, "Straight")

    def test_three_of_a_kind(self):
        hole = [C('A', 'hearts'), C('A', 'diamonds')]
        comm = [C('A', 'clubs'), C('K', 'spades'), C('Q', 'hearts'), C('2', 'clubs'), C('3', 'diamonds')]
        score, name = HandEvaluator.evaluate(hole, comm)
        self.assertGreaterEqual(score, 30_000_000_000)
        self.assertLess(score, 40_000_000_000)
        self.assertEqual(name, "Three of a Kind")

    def test_two_pair(self):
        hole = [C('A', 'hearts'), C('A', 'diamonds')]
        comm = [C('K', 'clubs'), C('K', 'spades'), C('Q', 'hearts'), C('2', 'clubs'), C('3', 'diamonds')]
        score, name = HandEvaluator.evaluate(hole, comm)
        self.assertGreaterEqual(score, 20_000_000_000)
        self.assertLess(score, 30_000_000_000)
        self.assertEqual(name, "Two Pair")

    def test_pair(self):
        hole = [C('A', 'hearts'), C('A', 'diamonds')]
        comm = [C('K', 'clubs'), C('Q', 'spades'), C('J', 'hearts'), C('2', 'clubs'), C('3', 'diamonds')]
        score, name = HandEvaluator.evaluate(hole, comm)
        self.assertGreaterEqual(score, 10_000_000_000)
        self.assertLess(score, 20_000_000_000)
        self.assertEqual(name, "Pair")

    def test_high_card(self):
        hole = [C('A', 'hearts'), C('K', 'diamonds')]
        comm = [C('Q', 'clubs'), C('9', 'spades'), C('7', 'hearts'), C('2', 'clubs'), C('3', 'diamonds')]
        score, name = HandEvaluator.evaluate(hole, comm)
        self.assertLess(score, 10_000_000_000)
        self.assertEqual(name, "High Card")

    def test_tie_breaking(self):
        # Two straights, one higher
        hole1 = [C('3', 'c'), C('4', 'd')]
        comm1 = [C('5', 'h'), C('6', 's'), C('7', 'c'), C('2', 'c'), C('K', 'd')]
        
        hole2 = [C('2', 'c'), C('3', 'd')]
        comm2 = [C('4', 'h'), C('5', 's'), C('6', 'c'), C('8', 'c'), C('K', 'd')]
        
        score1, _ = HandEvaluator.evaluate(hole1, comm1)
        score2, _ = HandEvaluator.evaluate(hole2, comm2)
        
        self.assertGreater(score1, score2)

if __name__ == '__main__':
    unittest.main()
