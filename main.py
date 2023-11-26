from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import pandas as pd
import threading
import time


class TradeApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = {}

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


def make_stock(symbol, sec_type="STK", currency="USD", exchange="ISLAND"):
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


def websocket_con():
    app.run()


def data_to_dataframes(symbols, trade_app):
    """returns extracted historical data in dataframe format"""
    df_data = {}
    for idx, symbol in enumerate(symbols):
        df_data[symbol] = pd.DataFrame(trade_app.data[idx])
        df_data[symbol].set_index("Date", inplace=True)
    return df_data


app = TradeApp()
app.connect(
    host="127.0.0.1", port=4001, clientId=23
)
con_thread = threading.Thread(target=websocket_con, daemon=True)
con_thread.start()
time.sleep(1)  # some latency added to ensure that the connection is established

# ##### fetch historical data for stocks
tickers = ["META", "AMZN", "INTC"]
for idx, ticker in enumerate(tickers):
    fetch_historical_data(idx, make_stock(ticker), "2 D", "5 mins")
    time.sleep(5)

# ### extract and store historical data in dataframe
historical_data = data_to_dataframes(tickers, app)
for ticker, data in historical_data.items():
    data.to_csv(ticker + ".csv")
