import sys
import ztom
from ztom import *
import urllib.parse
import copy
import cli
from typing import List
import time
import tkgpro
from tkgpro import ThresholdRecoveryOrder, MakerStopLossOrder
import tkgtri
from tkgtri import rest_server
import pytz


def get_order(handler):
    key = urllib.parse.unquote(handler.path[7:])  # /order/id  - di starts from 7th position
    return key


def set_order(handler):
    global bot

    # key = urllib.parse.unquote(handler.path[8:])
    payload = handler.get_payload()

    bot.log(bot.LOG_INFO, "Creating order")
    bot.log(bot.LOG_INFO, payload)

    symbol = payload["symbol"]
    start_currency = payload["start_cur"]
    dest_currency = payload["dest_cur"]
    start_amount = float(payload["start_amount"])
    best_dest_amount = float(payload["best_dest_amount"])
    taker_price_threshold = bot.taker_price_threshold

    maker_price_threshold = bot.maker_stop_loss["maker_price_threshold"]
    maker_order_max_updates = bot.maker_stop_loss["maker_order_max_updates"]

    force_taker_updates = bot.maker_stop_loss["force_taker_updates"]
    taker_price_threshold = bot.maker_stop_loss["taker_price_threshold"]
    taker_order_max_updates = bot.maker_stop_loss["taker_order_max_updates"]
    threshold_check_after_updates = bot.maker_stop_loss["threshold_check_after_updates"]

    cancel_threshold = om.exchange.markets[symbol]["limits"]["amount"]["min"] * 1.01

    # recovery_order = ThresholdRecoveryOrder(symbol, start_currency, start_amount, dest_currency,
    #                                         best_dest_amount, taker_price_threshold=taker_price_threshold,
    #                                         cancel_threshold=cancel_threshold,
    #                                         max_best_amount_order_updates=bot.order_update_total_requests,
    #                                         max_order_updates=bot.max_order_update_attempts)

    recovery_order = MakerStopLossOrder.create_from_start_amount(
        symbol=symbol,
        start_currency=start_currency,
        start_amount=start_amount,
        dest_currency=dest_currency,
        target_amount=best_dest_amount,
        cancel_threshold=cancel_threshold,
        maker_price_threshold=maker_price_threshold,
        maker_order_max_updates=maker_order_max_updates,
        force_taker_updates=force_taker_updates,
        taker_price_threshold=taker_price_threshold,
        taker_order_max_updates=taker_order_max_updates,
        threshold_check_after_updates=threshold_check_after_updates
    )

    om.add_order(recovery_order)

    # adding data for report associated with order
    om.set_order_supplementary_data(recovery_order, {"deal-uuid": payload["deal-uuid"], "leg": payload["leg"]})

    return "ok"


def report_closed_orders(tribot: Bot, order_manager: ActionOrderManager, closed_orders: List[ActionOrder]):
    tribot.log(tribot.LOG_INFO, "Reporting closed orders....")

    recovery_report = list()

    for order in closed_orders:
        tribot.log(tribot.LOG_INFO, ".. order ID: {}".format(order.id))

        order_supplementary_data = copy.copy(order_manager.supplementary[order.id])

        report_data = dict()
        report_data["timestamp"] = order.timestamp
        report_data["timestamp_close"] = order.timestamp_close
        report_data["result-fact-diff"] = order.filled_dest_amount
        report_data["status"] = "Recovered{}".format(order_supplementary_data["leg"])
        report_data["leg{}-price-fact".format(order_supplementary_data["leg"])] = order.filled_price
        report_data["deal-uuid"] = order_supplementary_data["deal-uuid"]
        report_data["server-id"] = tribot.server_id
        report_data["exchange-id"] = tribot.exchange_id
        report_data["om-server"] = "Recover1"
        report_data["cur1"] = order.dest_currency
        report_data["dest_currency"] = order.dest_currency
        report_data["start_currency"] = order.start_currency
        report_data["target_price"] = order.price
        report_data["state"] = order.state
        # report_data["target_amount"] = order.best_dest_amount
        report_data["start_amount"] = order.start_amount
        report_data["target_amount"] = order.dest_amount
        report_data["filled_amount"] = order.filled_dest_amount
        report_data["order_type"] = str(type(order))
        report_data["tags"] = " ".join(order.tags) if len(order.tags) > 0 else None

        if tribot.reporter is not None:

            for rd in report_data:
                tribot.reporter.set_indicator(rd, report_data[rd])

            try:
                tribot.log(tribot.LOG_INFO, "Sending report  order {} to INFLUX".format(order.id))
                tribot.reporter.push_to_influx()  # bad we could report all at once
                tribot.log(tribot.LOG_INFO, "..ok")
            except Exception as e:
                tribot.log(tribot.LOG_ERROR, "Error sending report data for order {}".format(order.id))
                tribot.log(tribot.LOG_ERROR, "Exception: {}".format(type(e).__name__))
                tribot.log(tribot.LOG_ERROR, "Exception body:", e.args)

        report_data["trade_order_internal_id"] = [o.internal_id for o in order.orders_history]

        # if tribot.mongo["enabled"]:
        #     try:
        #         tribot.log(tribot.LOG_INFO, "Sending report RecoveryOrder id: {} to Mongo".format(order.id))
        #         tribot.mongo_reporter.push_report(report_data, "tri_results")
        #         tribot.log(tribot.LOG_INFO, "..ok")
        #
        #     except Exception as e:
        #         tribot.log(tribot.LOG_ERROR, "Error sending report data for order {}".format(order.id))
        #         tribot.log(tribot.LOG_ERROR, "Exception: {}".format(type(e).__name__))
        #         tribot.log(tribot.LOG_ERROR, "Exception body:", e.args)
        #
        #     trade_orders_report = order.closed_trade_orders_report()
        #
        #     try:
        #         tribot.log(tribot.LOG_INFO, "Sending trade orders report to Mongo...")
        #         tribot.mongo_reporter.push_report(trade_orders_report, "trade_orders")
        #         tribot.log(tribot.LOG_INFO, "..ok")
        #     except Exception as e:
        #         tribot.log(tribot.LOG_ERROR, "Error sending report data for order {}".format(order.id))
        #         tribot.log(tribot.LOG_ERROR, "Exception: {}".format(type(e).__name__))
        #         tribot.log(tribot.LOG_ERROR, "Exception body:", e.args)

        # SQL Report
        if tribot.sqla_reporter is not None:

            tribot.log(tribot.LOG_INFO, "Sending  SQL report...")
            if tribot.sqla_reporter.session is None:
                tribot.sqla_reporter.new_session()
                tribot.log(tribot.LOG_INFO, ".. new Session created ")

            deal_uuid = order_supplementary_data["deal-uuid"]
            # deal_report = tribot.sqla_reporter.session.query(DealReport).filter_by(deal_uuid=deal_uuid)\
            #     .first()  # type: DealReport

            tribot.log(tribot.LOG_INFO, "... preparing deal report")
            deal_report = DealReport(
                timestamp=datetime.now(tz=pytz.timezone("UTC")),
                timestamp_start=datetime.fromtimestamp(order.timestamp, tz=pytz.timezone("UTC")),
                exchange=tribot.exchange.exchange_id,
                instance=tribot.server_id,
                server=tribot.server_id,
                deal_type="triarb",
                deal_uuid=deal_uuid,
                status="Recovered{}".format(order_supplementary_data["leg"]),
                currency=order.dest_currency,
                start_amount=0.0,
                result_amount=order.filled_dest_amount,
                gross_profit=order.filled_dest_amount,
                net_profit=0.0,
                config={},
                deal_data=report_data)

            tribot.sqla_reporter.session.add(deal_report)

            if order.start_amount - order.filled_start_amount > 0:
                tribot.log(bot.LOG_INFO, "... preparing remainings record")
                remaining = Remainings(
                    exchange=tribot.exchange.exchange_id,
                    account="account1",
                    timestamp=datetime.now(tz=pytz.timezone("UTC")),
                    action="ADD",
                    currency=order.start_currency,
                    amount_delta=order.start_amount - order.filled_start_amount,
                    target_currency=order.dest_currency,
                    target_amount_delta=core.convert_currency(start_currency=order.start_currency,
                                                              start_amount=order.start_amount-order.filled_start_amount,
                                                              dest_currency=order.dest_currency,
                                                              symbol=order.symbol,
                                                              price=order.price),
                    # target_amount_delta=order.dest_amount - order.filled_dest_amount,
                    symbol=order.symbol
                )

                tribot.log(bot.LOG_INFO, remaining)
                tribot.sqla_reporter.session.add(remaining)

            try:
                tribot.sqla_reporter.session.commit()
                tribot.log(tribot.LOG_INFO, "... committed")

            except Exception as e:
                tribot.log(tribot.LOG_ERROR, "Error sending SQL report data for order {}".format(order.id))
                tribot.log(tribot.LOG_ERROR, "Exception: {}".format(type(e).__name__))
                tribot.log(tribot.LOG_ERROR, "Exception body:", e.args)

                tribot.sqla_reporter.session.rollback()
                tribot.log(tribot.LOG_ERROR, "SQL session rolled back")

            tribot.log(tribot.LOG_INFO, "... preparing orders report")
            i = 0
            for trade_order in order.orders_history:
                tribot.sqla_reporter.session.add(
                    TradeOrderReport.from_trade_order(trade_order,
                                                      timestamp=datetime.now(tz=pytz.timezone("UTC")),
                                                      deal_uuid=deal_uuid,
                                                      action_order_id=order.id,
                                                      supplementary={"order_num": i, "deal_state": "recovery",
                                                                     "leg": order_supplementary_data["leg"]}))
                i += 1

            try:
                tribot.sqla_reporter.session.commit()
                tribot.log(tribot.LOG_INFO, "... committed")
            except Exception as e:
                tribot.log(tribot.LOG_ERROR, "Error sending SQL report data for order {}".format(order.id))
                tribot.log(tribot.LOG_ERROR, "Exception: {}".format(type(e).__name__))
                tribot.log(tribot.LOG_ERROR, "Exception body:", e.args)

                tribot.sqla_reporter.session.rollback()
                tribot.log(tribot.LOG_ERROR, "SQL session rolled back")


def worker():
    if len(om.get_open_orders()) > 0:

        om.proceed_orders()

        # this code is valid for offline only! Do not use for online force-closing ActionOrder!!!!
        if bot.offline and bot.force_cancel > 0:

            open_orders = om.get_open_orders()
            for order in open_orders:
                if order.state == "taker" and (order.filled_dest_amount / order.dest_amount > bot.force_cancel):
                    bot.log(bot.LOG_INFO, f"Order {order.id} was forced to close")
                    order.close_order()
                    if order not in om._last_update_closed_orders:
                        om._last_update_closed_orders.append(order)

        bot.log(bot.LOG_INFO, "Sleeping after orders proceeding for {}s...".format(bot.om_proceed_sleep))

        time.sleep(bot.om_proceed_sleep)  # workaround
        closed_orders = om.get_closed_orders()
        if closed_orders:
            report_closed_orders(bot, om, closed_orders)
    else:
        # bot.log(bot.LOG_INFO, "No open orders")
        # time.sleep(5)
        # bot.log(bot.LOG_INFO, "Have slept")
        pass


Bot.recovery_server = ""
bot = Bot("", "server.log")
bot.force_cancel = 0.0

bot.maker_stop_loss = dict()
"""
   params for  MakerStopLossOrder
   maker_price_threshold: float = 0.005,
   maker_order_max_updates: int = 50,
   force_taker_updates: int = 500,
   taker_price_threshold: float = -0.01,
   taker_order_max_updates: int = 10,
   threshold_check_after_updates: int = 5

"""

bot.get_cli_parameters = cli.get_cli_parameters
bot.set_from_cli(sys.argv[1:])

bot.om_proceed_sleep = 0.0  # init om_proceed sleep parameter
bot.taker_price_threshold = 0.0  # set to load the parameter from config file

bot.load_config_from_file(bot.config_filename)
bot.set_from_cli(sys.argv[1:])

bot.log(bot.LOG_INFO, "Starting server...")
bot.log(bot.LOG_INFO, "Config filename: {}".format(bot.config_filename))
bot.log(bot.LOG_INFO, "Exchange ID:" + bot.exchange_id)
if bot.offline:
    bot.log(bot.LOG_INFO, "Offline Mode")

bot.log(bot.LOG_INFO, "OM sleep time: {}".format(bot.om_proceed_sleep))

# init the remote reporting
try:
    bot.init_remote_reports()
except Exception as e:
    bot.log(bot.LOG_ERROR, "Error Report DB connection {}".format(bot.exchange_id))
    bot.log(bot.LOG_ERROR, "Exception: {}".format(type(e).__name__))
    bot.log(bot.LOG_ERROR, "Exception body:", e.args)
    bot.log(bot.LOG_INFO, "Continue....", e.args)

bot.init_exchange()
if bot.offline:
    bot.log(bot.LOG_INFO, "Running in offline mode")
    bot.log(bot.LOG_INFO, "Loading from offline test_data/markets.json test_data/tickers.csv")
    bot.exchange.set_offline_mode("test_data/markets.json", "test_data/tickers.csv")
    bot.exchange.offline_use_last_tickers = True

else:
    bot.exchange.init_async_exchange()

bot.load_markets()

ActionOrderManager.log = bot.log  # override order manager logger to the bot logger
ActionOrderManager.LOG_INFO = bot.LOG_INFO
ActionOrderManager.LOG_ERROR = bot.LOG_ERROR
ActionOrderManager.LOG_DEBUG = bot.LOG_DEBUG
ActionOrderManager.LOG_CRITICAL = bot.LOG_CRITICAL

om = ActionOrderManager(bot.exchange, bot.max_order_update_attempts, request_sleep=bot.request_sleep)

ActionOrderManager._get_trade_results = bot.get_trade_results  # set get trades from bot ;)

rest_server.service_worker = worker
rest_server.routes = {
    r'^/orders': {'GET': rest_server.get_records, 'media_type': 'application/json'},
    r'^/order/': {'GET': get_order, 'PUT': set_order, 'DELETE': rest_server.delete_record,
                  'media_type': 'application/json'}}

rest_server.poll_interval = 0.01
rest_server.port = int(bot.port)
rest_server.main(sys.argv[1:])
sys.exit()
