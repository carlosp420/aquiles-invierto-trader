from datetime import datetime

import pandas as pd
import requests
from ibapi.contract import Contract

from aquiles_enums import Status, Right
from orders import place_order


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


def get_days_since_open(sell_date):
    sell_date = datetime.strptime(sell_date, "%Y-%m-%d").date()
    today = datetime.today().date()
    return (today - sell_date).days


BUY_PERCENTAGES = {
    '1': 0.75,  # Buy To Close if price dropped 25%
    '7': 0.50,  # Buy To Close if price dropped 50%
    '8': 0.30,  # Buy To Close if price dropped 70%
}


def close_open_positions_cloud(app, dry_run=False):
    """Read downloaded CSV of Options Trading google sheet

    Extract the PUT options and their date they were sold to open.
    Place order to close contracts that have dropped in price.

    Close if price dropped 25% within 1 day, 30% within 7 days, 50% after 7 days.
    """
    url = "https://tracker.aquilesinvierto.com/tracker/trades_json/"
    response = requests.get(url)
    data = response.json()

    for idx, row in enumerate(data):
        if row["status"] == Status.closed.value:
            # closed position
            continue
        if row["type"] == "BUY":
            # this is a protective put
            continue

        ticker = row['symbol']
        avg_cost = float(row['sell_price'])
        days_since_open = get_days_since_open(row['sell_date'])
        num_contracts = int(row['num_of_contracts'])

        buy_price = get_buy_price(avg_cost, days_since_open, ticker)

        expiry = datetime.strptime(row['last_trade_date_or_contract_month'], "%Y-%m-%d").strftime('%Y%m%d')
        strike = row['strike']
        right = Right(row['right']).name.upper()
        contract = make_option(ticker, expiry, strike, right)

        print(f"BUY {num_contracts} {ticker} {expiry} {strike} {right} {buy_price}")

        if not dry_run:
            order_id = app.nextValidOrderId

            place_order(
                app, order_id, action="BUY", limit_price=buy_price, contract=contract, num_contracts=num_contracts
            )
            app.nextOrderId()


def close_open_positions_csv(app, dry_run=False):
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
        days_since_open = int(row['Days since open'])
        num_contracts = abs(int(row['Num Contratos']))

        buy_price = get_buy_price(avg_cost, days_since_open, ticker)

        expiry = datetime.strptime(row['Ticker'].split(" ")[1], "%b%d'%y").strftime('%Y%m%d')
        strike = row['Ticker'].split(" ")[2]
        right = row['Ticker'].split(" ")[3]
        contract = make_option(ticker, expiry, strike, right)

        print(f"BUY {num_contracts} {ticker} {expiry} {strike} {right} {buy_price}")

        if not dry_run:
            order_id = app.nextValidOrderId

            place_order(
                app, order_id, action="BUY", limit_price=buy_price, contract=contract, num_contracts=num_contracts
            )
            app.nextOrderId()


def get_buy_price(avg_cost, days_since_open, ticker):
    if days_since_open < 1:
        buy_price = avg_cost * BUY_PERCENTAGES['1']  # Buy To Close if price dropped 25%
    elif days_since_open <= 7:
        buy_price = avg_cost * BUY_PERCENTAGES['7']  # Buy To Close if price dropped 50%
    else:
        # days_since_sold > 7:
        buy_price = avg_cost * BUY_PERCENTAGES['8']  # Buy To Close if price dropped 70%

    if ticker in ['CPER', 'EZU', 'SPX']:
        buy_price = round(buy_price, 1)
    else:
        buy_price = round(buy_price, 2)

    return buy_price
