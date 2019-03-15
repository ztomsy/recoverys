import tkgcore as tkg
from tkgcore import Bot
from tkgcore.trade_order_manager import *
import sys

start_cur = "ETH"
dest_cur = "BTC"
start_amount = 0.02
offline = True


Bot.recovery_server = ""
Bot.taker_price_threshold = 0.0

bot = Bot("_config_default.json", "server.log")
bot.load_config_from_file(bot.config_filename)

bot.log(bot.LOG_INFO, "Starting test...")
bot.init_exchange()

bot.offline = offline

if bot.offline:
    bot.log(bot.LOG_INFO, "Running in offline mode")
    bot.log(bot.LOG_INFO, "Loading from offline test_data/markets.json test_data/tickers.csv")
    bot.exchange.set_offline_mode("test_data/markets.json", "test_data/tickers.csv")
else:
    bot.exchange.init_async_exchange()

bot.load_markets()
if bot.offline:
    bot.fetch_tickers()  # for getting data for order books

symbol = tkg.core.get_symbol(start_cur, dest_cur, bot.markets)

tkg.ActionOrderManager.log = bot.log  # override order manager logger to the bot logger
tkg.ActionOrderManager.LOG_INFO = bot.LOG_INFO
tkg.ActionOrderManager.LOG_ERROR = bot.LOG_ERROR
tkg.ActionOrderManager.LOG_DEBUG = bot.LOG_DEBUG
tkg.ActionOrderManager.LOG_CRITICAL = bot.LOG_CRITICAL

ob_array = bot.exchange.fetch_order_book(symbol, 100)
ob = tkg.OrderBook(symbol, ob_array["asks"], ob_array["bids"])

price = ob.get_depth_for_destination_currency(start_amount, dest_cur).total_price
order1 = tkg.ActionOrder.create_from_start_amount(symbol, start_cur, start_amount, dest_cur, price)

bot.log(bot.LOG_INFO, "From {} -{}-> {}".format(order1.start_currency, order1.side, order1.dest_currency))
bot.log(bot.LOG_INFO, "Price: {}".format(price))


om = tkg.ActionOrderManager(bot.exchange)
om.request_trades = False
om.add_order(order1)

while len(om.get_open_orders()) > 0:
    om.proceed_orders()


trade_order1 = order1.orders_history[-1]
fee_dest_cur = trade_order1.fees[dest_cur]["amount"] if dest_cur in trade_order1.fees else 0.0
print("Order completed: Filled dest amount {} {} fee {} ".format(
    trade_order1.filled_dest_amount, trade_order1 .dest_currency, fee_dest_cur))

print("Total result amount of {}: {}". format(trade_order1 .dest_currency,
                                              bot.exchange.price_to_precision(order1.symbol,
                                                                    trade_order1.filled_dest_amount - fee_dest_cur)))

payload = dict()

payload = {'start_cur': dest_cur,
           'best_dest_amount': start_amount,
           'timestamp': 1536664829.9662569,
           'leg': 2,
           'start_amount': bot.exchange.price_to_precision(order1.symbol, order1.filled_dest_amount - fee_dest_cur),
           'deal-uuid': 'test-server',
           'dest_cur': start_cur,
           'symbol': symbol}

result = tkg.rest_server.rest_call_json("http://localhost:8080/order/", payload, "PUT")
bot.log(bot.LOG_INFO, "Result of request: {}".format(result))

