import time

from ibapi.contract import Contract
from ibapi.order import Order


def place_order(
    app, order_id, action: str, limit_price, contract: Contract, order_type: str = "LMT",
    num_contracts: int = 1
) -> int:
    """places order and returns order id

    By default, orders are valid only for the trading session on the day the order was placed.

    action: BUY, SELL
    order_type: LMT, MKT, STP
    """
    order = Order()
    order.action = action
    order.orderType = order_type
    order.totalQuantity = num_contracts
    order.lmtPrice = limit_price

    app.placeOrder(order_id, contract, order)
    time.sleep(5)
    return order_id
