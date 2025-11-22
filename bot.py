from time import sleep
import logging
import sys
from datetime import datetime, timedelta

from imcity_template import BaseBot, Side, OrderRequest, OrderBook, Order
from estimates.safety_net import *
from estimates.weather_forecast import get_3_weather_prediction


# colored stdout logging
logger = logging.getLogger("RoboTrader")
logger.setLevel(logging.INFO)
logger.handlers[:] = []  # clear any existing handlers

stream_handler = logging.StreamHandler(sys.stdout)

class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: "\033[36m",    # cyan
        logging.INFO: "\033[32m",     # green
        logging.WARNING: "\033[33m",  # yellow
        logging.ERROR: "\033[31m",    # red
        logging.CRITICAL: "\033[35m", # magenta
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelno, self.RESET)
        # colorize the level name only
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        return super().format(record)

formatter = ColorFormatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)
logger.propagate = False


EXPECTED_SETTLEMENT = {
    # '1_Eisbach': int(predict_market_1()),
    '2_Eisbach_Call': int(predict_market_2()),
    '3_Weather': int(get_3_weather_prediction()),
    # '4_Weather': 8545,
    # '5_Flights': 2499,
    # '6_Airport': 0,
    '7_ETF': int(predict_market_7()),
    # '8_ETF_Strangle': 0,
}
logger.info(f"Expected Settlements: {EXPECTED_SETTLEMENT}")


def update_settlement():
    # EXPECTED_SETTLEMENT['1_Eisbach'] = int(predict_market_1())
    EXPECTED_SETTLEMENT['2_Eisbach_Call'] = int(predict_market_2())
    EXPECTED_SETTLEMENT['3_Weather'] = int(get_3_weather_prediction())
    sleep(1)
    EXPECTED_SETTLEMENT['7_ETF'] = int(predict_market_7())
    logger.info(f"Expected Settlements: {EXPECTED_SETTLEMENT}")


class RoboTrader(BaseBot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.new_orders: list[OrderRequest] = []
        self.last_trade_time = datetime.now()

        self.positions = {}
        self.position_limit = 100
        self.base_order_volume = 1
        self.base_spread_percentage = 10

        self.orderbook_estimate = {} # product_name -> (best_bid, best_ask, mid_price, spread)

    def update_position(self, product, volume):
        if product not in self.positions:
            self.positions[product] = 0
        self.positions[product] += volume

        # TODO: Check back with .request_positions() if accurate

    def main(self):
        self.get_orderbooks()
        sleep(10)

    # INCOMING - Trade Notifications
    def on_trades(self, trades: list[dict]):
        sleep(1)
        for trade in trades:
            product = trade['product']
            volume = trade['volume']
            price = trade['price']
            

            if trade['buyer'] == self.username:
                # self.update_position(product, volume)
                logger.critical(f"[TRADE] BUY on {product}: #{volume} @ {price}. Pos: {self.positions[product]}")
            elif trade['seller'] == self.username:
                # self.update_position(product, -volume)
                logger.critical(f"[TRADE] SELL on {product}: #{volume} @ {price}. Pos: {self.positions[product]}")

            self.positions = self.request_positions()
            logger.info(f"Updated Positions: {self.positions}")


    # INCOMING - Order Book Updates
    def on_orderbook(self, orderbook):
        product = orderbook.product
        best_bid = orderbook.buy_orders[0].price
        best_ask = orderbook.sell_orders[0].price
        mid_price = (best_bid + best_ask) / 2.0
        expected_settlement = EXPECTED_SETTLEMENT.get(product, "NO EXP. SETTLEMENT")
        if product not in EXPECTED_SETTLEMENT:
            return

        self.orderbook_estimate[product] = (best_bid, best_ask, mid_price, best_ask - best_bid)

        # print(f"[ORDERBOOK {product}] Best Bid: {best_bid}, Best Ask: {best_ask}, Mid: {mid_price}, Expected Settlement: {expected_settlement}")
        # print("Orderbook Activity")
        logger.info(f"[ORDERBOOK {product}] Best Bid: {best_bid}, Best Ask: {best_ask}, Mid: {mid_price}, Expected Settlement: {expected_settlement}")
        logger.info(f"{self.orderbook_estimate}")

        self.trade()

    def get_orderbooks(self):
        for product in EXPECTED_SETTLEMENT.keys():
            resp = self.request_order_book_per_product(product)
            logger.info(f"Got orderbooks: {resp}")

    # TRADING LOGIC
    def trade(self):
        for product in self.orderbook_estimate:
            best_bid, best_ask, market_mid_price, market_spread = self.orderbook_estimate[product]
            current_pos = self.positions.get(product, 0)
            estimated_settlement = EXPECTED_SETTLEMENT.get(product, None)
            if not estimated_settlement:
                continue

            spread = estimated_settlement * (self.base_spread_percentage / 100)
            my_bid = int(estimated_settlement - (spread / 2))
            my_ask = int(estimated_settlement + (spread / 2))

            order_volume = self.base_order_volume

            # Safety Checks
            can_buy = current_pos + order_volume <= self.position_limit
            can_sell = current_pos - order_volume >= -self.position_limit

            bid_would_execute = my_bid >= best_ask
            ask_would_execute = my_ask <= best_bid
            bid_is_highest = my_bid >= best_bid
            ask_is_lowest = my_ask <= best_ask

            logger.warning(f"[{product}] MARKET IS Bid: {best_bid}, Ask: {best_ask}, Mid: {market_mid_price}, Spread: {market_spread}")
            if can_buy:
                self.add_order(product, Side.BUY, my_bid, order_volume)
                logger.warning(f"[ORDER] Placing BUY order for {product}: #{order_volume} @ {my_bid}")
                # logger.warning(f"Would place BUY order for {product}: #{order_volume} @ {my_bid} for est. settlement {estimated_settlement}")
                # logger.warning(f"Our bid is {my_bid-best_bid} higher than market --> Would Execute: {bid_would_execute}, Is Highest: {bid_is_highest}")

            if can_sell:
                self.add_order(product, Side.SELL, my_ask, order_volume)
                logger.warning(f"[ORDER] Placing SELL order for {product}: #{order_volume} @ {my_ask}")
                # logger.warning(f"Would place SELL order for {product}: #{order_volume} @ {my_ask} for est. settlement {estimated_settlement}")
                # logger.warning(f"Our ask is {best_ask-my_ask} lower than market --> Would Execute: {ask_would_execute}, Is Lowest: {ask_is_lowest}")

    # OUTGOING - Place Orders
    def add_order(self, product, side: Side, price, volume):
        if datetime.now() - self.last_trade_time < timedelta(seconds=1):
            self.add_order_to_backlog(product, side, price, volume)
        else:
            self.add_order_to_backlog(product, side, price, volume)
            self.execute_orders()
            self.last_trade_time = datetime.now()

    def add_order_to_backlog(self, product, side: Side, price, volume):
        order_request = OrderRequest(
            product=product,
            side=side,
            price=price,
            volume=volume,
        )

        self.new_orders.append(order_request)
        logger.info(f"[ORDER ADDED] {side} {product} #{volume} @ {price}")


    def execute_orders(self):
        resp = self.send_mass_orders(self.new_orders)
        self.new_orders = []


if __name__ == "__main__":
    import os
    # from dotenv import load_dotenv

    # load_dotenv()

    TEST_EXCHANGE = os.environ.get("IMCITY_TEST_EXCHANGE", "http://ec2-52-31-108-187.eu-west-1.compute.amazonaws.com")
    REAL_EXCHANGE = os.environ.get("IMCITY_REAL_EXCHANGE", "http://ec2-18-203-201-148.eu-west-1.compute.amazonaws.com")
    USERNAME = os.environ.get("IMCITY_USERNAME")
    PASSWORD = os.environ.get("IMCITY_PASSWORD")

    if not USERNAME or not PASSWORD:
        raise RuntimeError("Environment variables IMCITY_USERNAME and IMCITY_PASSWORD must be set.")

    try:
        bot = RoboTrader(TEST_EXCHANGE, USERNAME, PASSWORD)
        
        # Sync positions on startup
        server_positions = bot.request_positions()
        if server_positions:
            bot.positions = server_positions
            logger.info(f"Initial Positions: {bot.positions}")
        
        bot.start()

        last_minute = None
        while True:
            now = datetime.now()
            if now.minute in {1, 16, 34, 46} and now.minute != last_minute:
                last_minute = now.minute
                logger.warning("Running 15-min clock-aligned task...")
                update_settlement()

            bot.main()


    except KeyboardInterrupt:
        bot.stop()
        print("Bot stopped.")