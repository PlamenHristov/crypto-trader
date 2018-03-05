import logging

import pandas as pd

from api.websocket_client import WebsocketClient
from exchanges.book import OrderBook
from api.public_client import PublicClient
from util.logger import Logger
import time


class GDaxOrderBook(WebsocketClient, OrderBook):
    exchange_name = 'Gdax'

    def __init__(self, client=PublicClient(), *args, **kwargs):
        actors = kwargs.pop('actors', None)
        WebsocketClient.__init__(self, *args, **kwargs)
        OrderBook.__init__(self, actors=actors, *args, **kwargs)
        self._client = client
        self.exchange_name = 'Gdax'

    def on_open(self):
        self._first_run = True
        Logger.info("-- Subscribed to OrderBook! --\n")

    def on_close(self):
        Logger.info("\n-- OrderBook Socket Closed! --")

    def on_message(self, message):
        if self._first_run:
            self.reset_book()
            self._first_run = False

        sequence = message['sequence']
        product_id = message['product_id']
        product_sequence = self.books[product_id]['sequence']

        if sequence <= product_sequence:
            Logger.info('Older message: {}\nSequence:{}'.format(message, self.books[product_id]['sequence']))
            # ignore older messages (e.g. before order book initialization from getProductOrderBook)
            return
        elif sequence > product_sequence + 1:
            self.on_sequence_gap(product_sequence, sequence)
            return

        msg_type = message['type']
        if msg_type == 'open':
            self.add(product_id, message)
        elif msg_type == 'done' and 'price' in message:
            self.remove(product_id, message)
        elif msg_type == 'match':
            self.match(product_id, message)
            self._current_ticker = message
        elif msg_type == 'change':
            self.change(product_id, message)

        self.books[product_id]['sequence'] = sequence
        self.send_book_to_subscribers()

    def _get_order_book_for_product(self, product_id, level=3):
        return self._client.get_product_order_book(product_id=product_id, level=level)

    def to_pandas_table(self, current_book):
        res_table = pd.DataFrame()

        for prod in current_book:
            bid_ask_data = current_book[prod]
            ask_tbl = pd.DataFrame(
                data=bid_ask_data['asks'],
                columns=['side', 'price', 'volume', 'order_id'],
                index=range(len(bid_ask_data['asks']))
            )
            bid_tbl = pd.DataFrame(
                data=bid_ask_data['bids'],
                columns=['side', 'price', 'volume', 'order_id'],
                index=range(len(bid_ask_data['bids']))
            )
            con_tbl = pd.concat([ask_tbl, bid_tbl, ], axis=0).reset_index(drop=True)
            con_tbl['ticker'] = prod
            res_table = pd.concat([con_tbl, res_table, ], axis=0).reset_index(drop=True)
        res_table['id'] = res_table.index
        return res_table


if __name__ == '__main__':
    import sys
    import time
    import datetime as dt


    class OrderBookConsole(GDaxOrderBook):
        ''' Logs real-time changes to the bid-ask spread to the console '''

        def __init__(self, product_id=None):
            super(OrderBookConsole, self).__init__(client=PublicClient(), products=["BTC-USD"])

            # latest values of bid-ask spread
            self._bid = None
            self._ask = None
            self._bid_depth = None
            self._ask_depth = None

        def on_message(self, message):
            super(OrderBookConsole, self).on_message(message)
            for product_id in self.product_ids:
                # Calculate newest bid-ask spread
                bid = self.get_bid(product_id, )
                bids = self.get_bids(product_id, bid)
                bid_depth = sum([b['size'] for b in bids])
                ask = self.get_ask(product_id, )
                asks = self.get_asks(product_id, ask)
                ask_depth = sum([a['size'] for a in asks])

                if self._bid == bid and self._ask == ask and self._bid_depth == bid_depth and self._ask_depth == ask_depth:
                    # If there are no changes to the bid-ask spread since the last update, no need to print
                    pass
                else:
                    # If there are differences, update the cache
                    self._bid = bid
                    self._ask = ask
                    self._bid_depth = bid_depth
                    self._ask_depth = ask_depth
                    print('gdax_order_book', '{} {} bid: {:.3f} @ {:.2f}\task: {:.3f} @ {:.2f}'.format(
                        dt.datetime.now(), product_id, bid_depth, bid, ask_depth, ask))


    order_book = OrderBookConsole()
    order_book.start()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        order_book.close()

    if order_book.error:
        sys.exit(1)
    else:
        sys.exit(0)
