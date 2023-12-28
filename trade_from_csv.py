import argparse

from ibapi.client import EClient
from ibapi.common import SetOfString, SetOfFloat
from ibapi.order import Order
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import pandas as pd
import threading
import time

from options import close_open_positions


class TradeApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = {}
        self.nextValidOrderId = None
        self.positions_df = pd.DataFrame(columns=["Account", "Symbol", "SecType", "Currency", "Position", "Avg cost"])
        self.option_chain_df = pd.DataFrame(columns=["Symbol", "Expiry", "Strike", "Right", "Type", "Multiplier"])

    def position(self, account, contract, position, avgCost):
        super().position(account, contract, position, avgCost)
        data = {
            "ConId": contract.conId,
            "LastTradeDateOrContractMonth": contract.lastTradeDateOrContractMonth,
            "Position": position,
            "Account": account,
            "Symbol": contract.symbol,
            "Avg cost": avgCost,
            "SecType": contract.secType,
            "Currency": contract.currency,
            "Strike": contract.strike,
            "Right": contract.right,
            "TradingClass": contract.tradingClass,
        }
        self.positions_df = self.positions_df._append(data, ignore_index=True)

    def securityDefinitionOptionParameter(self, reqId:int, exchange:str,
        underlyingConId:int, tradingClass:str, multiplier:str,
        expirations:SetOfString, strikes:SetOfFloat):
        super().securityDefinitionOptionParameter(
            reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes
        )
        data = {
            "reqId": reqId,
            "Exchange": exchange,
            "UnderlyingConId": underlyingConId,
            "Symbol": tradingClass,
            "Multiplier": multiplier,
            "Expiry": expirations,
            "Strike": strikes,
        }
        self.option_chain_df = self.option_chain_df._append(data, ignore_index=True)

    def nextValidId(self, orderId:int):
        """returns next valid order id"""
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print("nextValidId:", orderId)

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def historicalData(self, reqId, bar):
        if reqId not in self.data:
            self.data[reqId] = pd.DataFrame(
                [
                    {
                        "Date": bar.date,
                        "Open": bar.open,
                        "High": bar.high,
                        "Low": bar.low,
                        "Close": bar.close,
                        "Volume": bar.volume,
                    }
                ]
            )
        else:
            self.data[reqId] = pd.concat(
                (
                    self.data[reqId],
                    pd.DataFrame(
                        [
                            {
                                "Date": bar.date,
                                "Open": bar.open,
                                "High": bar.high,
                                "Low": bar.low,
                                "Close": bar.close,
                                "Volume": bar.volume,
                            }
                        ]
                    ),
                )
            )
        print(
            "reqID:{}, date:{}, open:{}, high:{}, low:{}, close:{}, volume:{}".format(
                reqId, bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume
            )
        )


def make_stock(symbol, sec_type="STK", currency="USD", exchange="SMART"):
    """

    :param symbol:
    :param sec_type:
    :param currency:
    :param exchange: SMART, NYSE, NASDAQ, ISLAND, ARCA, BATS, IEX, SECTORS, PSX, AMEX
        Use SMART when placing orders
        ISLAND to fetch historical data
    :return:
    """
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    return contract


def fetch_historical_data(req_num, contract, duration, candle_size):
    """extracts historical data

    data is stored in app.data
    """
    app.reqHistoricalData(
        reqId=req_num,
        contract=contract,
        endDateTime="",
        durationStr=duration,
        barSizeSetting=candle_size,
        whatToShow="ADJUSTED_LAST",
        useRTH=1,
        formatDate=1,
        keepUpToDate=False,
        chartOptions=[],
    )  # EClient function to request contract details


def data_to_dataframes(symbols, trade_app):
    """returns extracted historical data in dataframe format"""
    df_data = {}
    for idx, symbol in enumerate(symbols):
        df_data[symbol] = pd.DataFrame(trade_app.data[idx])
        df_data[symbol].set_index("Date", inplace=True)
    return df_data


def start_app():
    app = TradeApp()
    app.connect(
        host="127.0.0.1", port=7496, clientId=23
    )

    def websocket_con():
        app.run()

    con_thread = threading.Thread(target=websocket_con, daemon=True)
    con_thread.start()
    time.sleep(1)  # some latency added to ensure that the connection is established
    return app


def fetch_historical_stocks_data():
    # ##### fetch historical data for stocks
    tickers = ["META", "AMZN", "INTC"]
    for idx, ticker in enumerate(tickers):
        fetch_historical_data(idx, make_stock(ticker), "2 D", "5 mins")
        time.sleep(5)


def extract_store_historical_data():
    ### extract and store historical data in dataframe
    tickers = ["META", "AMZN", "INTC"]
    historical_data = data_to_dataframes(tickers, app)
    for ticker, data in historical_data.items():
        data.to_csv(ticker + ".csv")


def place_market_order(ticker) -> int:
    order = Order()
    order.action = "BUY"
    order.orderType = "MKT"
    order.totalQuantity = 1

    order_id = app.nextValidOrderId
    app.placeOrder(order_id, make_stock(ticker), order)
    time.sleep(5)
    return order_id


def cancel_order(order_id):
    app.cancelOrder(order_id, "")
    time.sleep(5)


def modify_order(ticker, order_id):
    order = Order()
    order.action = "BUY"
    order.orderType = "LMT"
    order.totalQuantity = 1
    order.lmtPrice = 101
    app.placeOrder(order_id, make_stock(ticker), order)
    time.sleep(5)


# order_id = app.nextValidOrderId
# order_id = place_order("AAPL", order_id)
# time.sleep(5)
# cancel_order(order_id)
# modify_order("AAPL", order_id)
# place_market_order("AAPL")
# app.reqPositions()
# time.sleep(5)
# pos_d = app.positions_df
# app.positions_df.to_json("positions.json")
# print(pos_d)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Aquiles trader")
    parser.add_argument('--dry-run', action=argparse.BooleanOptionalAction)
    dry_run = parser.parse_args().dry_run

    if not dry_run:
        app = start_app()
    else:
        app = None

    time.sleep(5)
    close_open_positions(app, dry_run)
    time.sleep(5)
