import gzip
import json
import time
import requests
import websocket
import threading
from queue import Queue
import hashlib
import hmac
from urllib.parse import urlencode, urlparse


request_client = requests

# Set up request_client.url
request_client.url = "https://api.coinex.com/v2"  

# Your credentials
access_id = "Replace with your access id"  
secret_key = "Replace with your secret key"  

class OrderBook:
    def __init__(self, signal_queue):
        self.ws = None
        self.order_bids = {}
        self.order_asks = {}
        self.trade_signal_history = []  # Store last N trade signals
        self.signal_window_size = 10  # Number of signals to track
        self.signal_queue = signal_queue  # Queue to send signals to the trading loop

    def depth_merge(self, order_dict, message):
        for item in message:
            if item[1] == '0':
                order_dict.pop(item[0], None)
            else:
                order_dict[item[0]] = item[1]

    def depth_process(self, message):
        clean = message['data']['is_full']
        depth_data = message['data']["depth"]

        if clean:
            self.order_bids = {item[0]: item[1] for item in depth_data['bids']}
            self.order_asks = {item[0]: item[1] for item in depth_data['asks']}
        else:
            if 'bids' in depth_data:
                self.depth_merge(self.order_bids, depth_data['bids'])
            if 'asks' in depth_data:
                self.depth_merge(self.order_asks, depth_data['asks'])

    def find_trade_signal(self):
     best_bid = max(float(price) for price in self.order_bids.keys())
     best_ask = min(float(price) for price in self.order_asks.keys())

     price_spread = best_ask - best_bid
     buyer_strength = sum(float(price) * float(amount) for price, amount in self.order_bids.items())
     seller_strength = sum(float(price) * float(amount) for price, amount in self.order_asks.items())

     if buyer_strength > seller_strength and price_spread < 0.1:
        return "buy"
     elif seller_strength > buyer_strength and price_spread < 0.1:
        return "sell"
     return None



    def count_signals(self):
        """ Count the number of 'sell' and 'buy' signals in history """
        sell_count = self.trade_signal_history.count("sell")
        buy_count = self.trade_signal_history.count("buy")
        return sell_count, buy_count

    def on_message(self, ws, message):
        message = gzip.decompress(message)
        message_json = json.loads(message)

        if message_json.get("method") == "depth.update":
            self.depth_process(message_json)
            signal = self.find_trade_signal()
            if signal:
                print(f"Trade Signal: {signal}")
                self.signal_queue.put(signal)  # Put signal in the queue

    def depth_subscribe(self):
        params = {
            "method": "depth.subscribe",
            "params": {"market_list": [["BTCUSDT", 10, "0", False]]},
            "id": 1,
        }
        self.ws.send(json.dumps(params))

    def on_open(self, ws):
        print("Connected to CoinEx WebSocket")
        self.depth_subscribe()

    def start(self):
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp("wss://socket.coinex.com/v2/futures",
                                         on_open=self.on_open,
                                         on_message=self.on_message)
        self.ws.run_forever()


# Modified RequestsClient for authentication
class RequestsClient:
    def __init__(self):
        self.access_id = access_id
        self.secret_key = secret_key
        self.url = request_client.url

    def gen_sign(self, method, request_path, body, timestamp):
        prepared_str = f"{method}{request_path}{body}{timestamp}"
        signature = hmac.new(
            bytes(self.secret_key, 'latin-1'), 
            msg=bytes(prepared_str, 'latin-1'), 
            digestmod=hashlib.sha256
        ).hexdigest().lower()
        return signature

    def get_common_headers(self, signed_str, timestamp):
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
            "X-COINEX-KEY": self.access_id,
            "X-COINEX-SIGN": signed_str,
            "X-COINEX-TIMESTAMP": timestamp,
        }
        return headers

    def request(self, method, url, params={}, data=""):
        req = urlparse(url)
        request_path = req.path
        timestamp = str(int(time.time() * 1000))
        
        if method.upper() == "GET":
            if params:
                request_path = request_path + "?" + urlencode(params)
            signed_str = self.gen_sign(method, request_path, body="", timestamp=timestamp)
            response = requests.get(url, params=params, headers=self.get_common_headers(signed_str, timestamp))

        else:
            signed_str = self.gen_sign(method, request_path, body=data, timestamp=timestamp)
            response = requests.post(url, data, headers=self.get_common_headers(signed_str, timestamp))

        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            return None
        return response


request_client = RequestsClient()

# OrderBook class and methods remain unchanged

# Variables globales pour suivre la position actuelle
current_position = None  # Peut être "buy", "sell", ou None
order_amount = "0.001"  # Quantité de la position (par exemple 0.0002 BTC)
entry_price = None  # Price at which position was opened


signal_count = 0
last_signal = None
trailing_stop_percent = 0.001  # 0.1%
entry_price = entry_price or 0  # Default to 0 if entry_price is None
stop_loss_price = entry_price * (1 - trailing_stop_percent)

def calculate_thresholds(volatility):
    return volatility * 2, volatility * -1  # Adjust thresholds

def monitor_open_position(order_book):
    """
    This function monitors an open position for profit/loss.
    Add your logic here for checking conditions.
    """
    current_price = get_current_price(order_book)
    if not current_price:
        print("Error fetching current price.")
        return

    # Example logic for monitoring profit/loss
    profit_threshold = entry_price * 1.05  # 5% profit
    loss_threshold = entry_price * 0.95   # 5% loss

    if current_price >= profit_threshold:
        print("Profit target reached. Closing position.")
        close_position("sell" if current_position == "buy" else "buy", order_book)
    elif current_price <= loss_threshold:
        print("Loss limit reached. Closing position.")
        close_position("sell" if current_position == "buy" else "buy", order_book)
    else:
        print(f"Current price: {current_price}. Monitoring position...")

def execute_trade(signal, order_book):
    global current_position, entry_price, last_signal

    current_price = get_current_price(order_book)
    if not current_price:
        print("Error fetching current price.")
        return

    if signal == last_signal:
        print(f"Repeated signal '{signal}' ignored.")
        return

    if current_position:
        if current_position != signal:
            print(f"Closing {current_position} due to opposing signal: {signal}.")
            close_position("sell" if current_position == "buy" else "buy", order_book)
            return
        else:
            print(f"Holding {current_position}. Monitoring for profit/loss.")
            monitor_open_position(order_book)
            return

    print(f"Opening new {signal} position.")
    order_side = "buy" if signal == "buy" else "sell"
    data = {
        "market": "BTCUSDT",
        "market_type": "FUTURES",
        "side": order_side,
        "type": "market",
        "amount": order_amount,
    }
    response = request_client.request("POST", f"{request_client.url}/futures/order", data=json.dumps(data))

    if response:
        current_position = signal
        entry_price = current_price
        print(f"{order_side.capitalize()} position opened at {current_price}.")
    else:
        print(f"Failed to open {order_side} position.")




def close_position(opposite_side, order_book):
    global current_position, entry_price

    if entry_price is None:
        print("No entry price available to calculate profit or loss.")
        return

    current_price = get_current_price(order_book)
    if not current_price:
        print("Error fetching the current price.")
        return

    # Calculate the profit or loss as a percentage
    if current_position == "buy":
        profit_or_loss = (current_price - entry_price) / entry_price
    elif current_position == "sell":
        profit_or_loss = (entry_price - current_price) / entry_price
    else:
        print("Unknown position type.")
        return

    # Define the thresholds for stop loss and take profit
    profit_threshold = 0.0019  # Take profit at 0.19%
    loss_threshold = -0.0009   # Stop loss at -0.09%

    print(f"PnL: {profit_or_loss * 100:.6f}%. Thresholds -> Profit: {profit_threshold}, Loss: {loss_threshold}")

    # Check if the profit or loss meets the threshold to close the position
    if profit_or_loss >= profit_threshold or profit_or_loss <= loss_threshold:
        reason = "Take profit" if profit_or_loss >= profit_threshold else "Stop loss"
        print(f"{reason}: Closing position with {opposite_side} order. Current PnL: {profit_or_loss * 100:.6f}%")

        data = {
            "market": "BTCUSDT",
            "market_type": "FUTURES",
            "side": opposite_side,  # Opposite side to close the position
            "type": "market",
            "amount": order_amount,
        }
        data = json.dumps(data)

        response = request_client.request("POST", f"{request_client.url}/futures/order", data=data)

        if response and response.status_code == 200:
            print(f"Position closed successfully: {response.json()}")
            current_position = None  # Reset the current position
            entry_price = None        # Reset the entry price
        else:
            print(f"Failed to close position. Response: {response.text}")
    else:
        print(f"Current PnL does not meet thresholds. PnL: {profit_or_loss * 100:.6f}%")







def get_current_price(order_book):
    """
    Fetches the current price from the order book (using the best bid or ask price).
    """
    if not order_book.order_bids or not order_book.order_asks:
        return None

    best_bid = max(order_book.order_bids.keys(), key=float)  # Highest bid price
    best_ask = min(order_book.order_asks.keys(), key=float)  # Lowest ask price

    # You could choose to use either the bid or ask price
    current_price = (float(best_bid) + float(best_ask)) / 2
    return current_price



def run():
    global current_position, entry_price, signal_count, last_signal  # Add last_signal to manage state

    signal_queue = Queue()
    order_book = OrderBook(signal_queue)  # Initialize order_book here
    order_book_thread = threading.Thread(target=order_book.start)
    order_book_thread.start()

    while True:
        if not signal_queue.empty():
            signal = signal_queue.get()

            # Check if a position is already open and if the signal matches the current position
            if current_position != signal:
                # Open a new position if the signal is different from the current position
                execute_trade(signal, order_book)

            # Optionally, close position after checking conditions (profit/loss)
            if current_position:
                signal_count += 1
                if signal_count >= 6 or last_signal != signal:
                    opposite_side = "sell" if current_position == "buy" else "buy"
                    close_position(opposite_side, order_book)

            last_signal = signal  # Update last_signal only after processing the current signal
        time.sleep(1)







# Run the main function
if __name__ == "__main__":
    run()
