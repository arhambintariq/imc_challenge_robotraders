import os
import time
from imcity_template import BaseBot, Side, OrderRequest, OrderBook, Order

# --- CONFIGURATION ---
# Read from environment. Use fallback for TEST_EXCHANGE, but require credentials.
TEST_EXCHANGE = os.environ.get("IMCITY_TEST_EXCHANGE", "http://ec2-52-31-108-187.eu-west-1.compute.amazonaws.com")
USERNAME = os.environ.get("IMCITY_USERNAME")
PASSWORD = os.environ.get("IMCITY_PASSWORD")

if not USERNAME or not PASSWORD:
    raise RuntimeError("Environment variables IMCITY_USERNAME and IMCITY_PASSWORD must be set.")

# --- FUNDAMENTAL DATA INPUTS (MANUAL OR SCRAPED) ---
# Update these values based on real-world data to give your bot an edge!
# If set to None, the bot will fall back to pure market making (blind).
FUNDAMENTALS = {
    "EISBACH_FLOW": 24.3,    # m3/s (Example)
    "EISBACH_LEVEL": 141,    # cm (Example)
    "MUNICH_TEMP": 30,       # Fahrenheit
    "MUNICH_HUMIDITY": 88,   # %
    "AIRPORT_METRIC": 0      # Current metric value
}

class InventorySkewBot(BaseBot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_action_time = 0
        
        # Inventory Management
        self.positions = {}
        self.position_limit = 200
        
        # Trading Settings
        self.order_volume = 1        # Low volume for safety
        self.base_spread = 2         # Tight spread
        self.skew_factor = 0.1       # Inventory skew intensity

    def calculate_fair_value(self, product_name):
        """
        Calculates the theoretical 'Fair Price' based on fundamental data.
        Returns None if insufficient data.
        """
        try:
            # --- Market 1: Eisbach Spot ---
            # Formula: Flow Rate * Water Level
            if product_name == "1_Eisbach":
                if FUNDAMENTALS["EISBACH_FLOW"] and FUNDAMENTALS["EISBACH_LEVEL"]:
                    fair_price = FUNDAMENTALS["EISBACH_FLOW"] * FUNDAMENTALS["EISBACH_LEVEL"]
                    return int(fair_price)

            # --- Market 7: Munich ETF ---
            # Formula: | 0.3*Flow + 0.1*Level + 0.2*Temp + 0.1*Hum + 0.3*Airport |
            elif product_name == "7_ETF":
                f = FUNDAMENTALS
                if all(v is not None for v in f.values()):
                    val = (0.3 * f["EISBACH_FLOW"] + 
                           0.1 * f["EISBACH_LEVEL"] + 
                           0.2 * f["MUNICH_TEMP"] + 
                           0.1 * f["MUNICH_HUMIDITY"] + 
                           0.3 * f["AIRPORT_METRIC"])
                    return abs(int(val))
            
            # --- Market 3: Weather (Simple Spot Proxy) ---
            # Note: Real formula is an accumulator over time. This is just a spot proxy.
            elif product_name == "3_Weather":
                 if FUNDAMENTALS["MUNICH_TEMP"] and FUNDAMENTALS["MUNICH_HUMIDITY"]:
                     # Proxy: Spot value of (Temp*2 + Humidity)
                     return int(FUNDAMENTALS["MUNICH_TEMP"] * 2 + FUNDAMENTALS["MUNICH_HUMIDITY"])

        except Exception as e:
            print(f"Error calculating fair value for {product_name}: {e}")
        
        return None

    def on_trades(self, trades: list[dict]):
        for trade in trades:
            product = trade['product']
            volume = trade['volume']
            price = trade['price']
            
            if product not in self.positions:
                self.positions[product] = 0

            if trade['buyer'] == self.username:
                self.positions[product] += volume
                print(f" [TRADE] Bought {volume} {product} @ {price}. Pos: {self.positions[product]}")
            elif trade['seller'] == self.username:
                self.positions[product] -= volume
                print(f" [TRADE] Sold {volume} {product} @ {price}. Pos: {self.positions[product]}")

    def on_orderbook(self, orderbook: OrderBook):
        if time.time() - self.last_action_time < 1.0:
            return

        product = orderbook.product
        current_pos = self.positions.get(product, 0)

        if not orderbook.buy_orders or not orderbook.sell_orders:
            return

        best_bid = orderbook.buy_orders[0].price
        best_ask = orderbook.sell_orders[0].price
        
        # --- STRATEGY CORE ---
        # 1. Try to calculate Fundamental Fair Value
        fundamental_price = self.calculate_fair_value(product)
        
        # 2. Fallback to Market Mid-Price if no fundamental data
        market_mid_price = (best_bid + best_ask) / 2.0
        
        if fundamental_price:
            # If we have a strong fundamental view, use it!
            mid_price = fundamental_price
            # print(f" [{product}] Using Fundamental Price: {mid_price} (Market: {market_mid_price:.1f})")
        else:
            # Otherwise, just market make around the current price
            mid_price = market_mid_price

        # 3. Apply Inventory Skew
        # Adjust our quote based on how much we are holding
        skew_intensity = 5 
        skew = -1 * (current_pos / self.position_limit) * skew_intensity

        theoretical_price = mid_price + skew
        
        my_bid = int(theoretical_price - (self.base_spread / 2))
        my_ask = int(theoretical_price + (self.base_spread / 2))

        # 4. Safety & Execution
        can_buy = current_pos + self.order_volume <= self.position_limit
        can_sell = current_pos - self.order_volume >= -self.position_limit

        try:
            if can_buy:
                # Don't bid higher than the best ask unless we intend to cross (taking liquidity)
                # If our fundamental price is WAY higher than market, we might actually want to cross.
                # For safety, we cap it at best_ask - 1 unless fundamental_price is present
                limit_price = my_bid
                if not fundamental_price and limit_price >= best_ask:
                    limit_price = best_ask - 1
                
                self.send_order(OrderRequest(
                    product=product,
                    price=limit_price,
                    volume=self.order_volume,
                    side=Side.BUY
                ))

            if can_sell:
                limit_price = my_ask
                if not fundamental_price and limit_price <= best_bid:
                    limit_price = best_bid + 1
                
                self.send_order(OrderRequest(
                    product=product,
                    price=limit_price,
                    volume=self.order_volume,
                    side=Side.SELL
                ))
            
            self.last_action_time = time.time()
            # Log fewer details to keep console clean, but show the important "Edge"
            if fundamental_price:
                diff = mid_price - market_mid_price
                print(f" [QUOTE] {product} | Edge: {diff:.1f} | Bid: {my_bid} Ask: {my_ask}")

        except Exception as e:
            print(f"Error sending order: {e}")

if __name__ == "__main__":
    print("Starting Fundamental + Inventory Bot...")
    try:
        bot = InventorySkewBot(TEST_EXCHANGE, USERNAME, PASSWORD)
        
        # Sync positions on startup
        server_positions = bot.request_positions()
        if server_positions:
            bot.positions = server_positions
            print(f"Initial Positions: {bot.positions}")
        
        bot.start()
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        bot.stop()
        print("Bot stopped.")