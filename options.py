from datetime import datetime

import pandas as pd
from ibapi.contract import Contract
from pandas import Series

from orders import place_order


def filter_put_options(positions: pd.DataFrame):
    """Filter out non-option positions"""
    return positions.query("Position < 0")


def compute_btc_put_price(option: Series):
    """Compute the price to buy back a short put"""
    sold_price = option["Avg cost"] / 100
    sold_date = pd.to_datetime(option["LastTradeDateOrContractMonth"])
    return option["Position"] * option["Avg cost"] * option["Multiplier"]


def make_option(symbol, expiry, strike, right, multiplier="100", exchange="SMART"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "OPT"
    contract.currency = "USD"
    contract.exchange = exchange
    contract.lastTradeDateOrContractMonth = expiry
    contract.strike = strike
    contract.right = right
    contract.multiplier = multiplier
    return contract


def close_open_positions(app):
    """Read downloaded CSV of Options Trading google sheet

    Extract the PUT options and their date they were sold to open.
    Place order to close contracts that have dropped in price.

    Close if price dropped 25% within 1 day, 30% within 7 days, 50% after 7 days.
    """
    df = pd.read_csv(
        "~/Downloads/Options trading Aquiles Invierto - Sheet1.csv", header=2
    )
    df = df.query("Status == 'O'")  # Only open positions

    for idx, row in df.iterrows():
        ticker = row['Ticker.1'].strip()
        avg_cost = float(row['Ticker'].split(" ")[-1])
        days_since_sold = int(row['Days since sold'])
        num_contracts = abs(int(row['Num Contratos']))

        if days_since_sold < 1:
            buy_price = avg_cost * 0.75  # Buy To Close if price dropped 25%
        elif days_since_sold <= 7:
            buy_price = avg_cost * 0.70  # Buy To Close if price dropped 30%
        else:
            # days_since_sold > 7:
            buy_price = avg_cost * 0.50  # Buy To Close if price dropped 50%

        if ticker in ['CPER']:
            buy_price = round(buy_price, 1)
        else:
            buy_price = round(buy_price, 2)

        expiry = datetime.strptime(row['Ticker'].split(" ")[1], "%b%d'%y").strftime('%Y%m%d')
        strike = row['Ticker'].split(" ")[2]
        right = row['Ticker'].split(" ")[3]
        contract = make_option(ticker, expiry, strike, right)

        order_id = app.nextValidOrderId

        print(f"\nBUY {num_contracts} {ticker} {expiry} {strike} {right} {buy_price} order_id: {order_id}")
        place_order(
            app, order_id, action="BUY", limit_price=buy_price, contract=contract, num_contracts=num_contracts
        )
        app.nextOrderId()
