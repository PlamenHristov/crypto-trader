from decimal import Decimal

import pandas as pd
from bintrees import RBTree

from app.api.public_client import PublicClient
from app.api.websocket_client import WebsocketClient
from app.util.logger import Logger


class GDaxOrderBook(WebsocketClient):
    def __init__(self, products=list('BTC-USD'), actors=list()):
        super(GDaxOrderBook, self).__init__(products=products)
        self.books = {
            prod: {
                "_asks": RBTree(),
                "_bids": RBTree(),
                "sequence": 0,
            }
            for prod in products
        }
        self._client = PublicClient()
        self._first_run = True
        self._current_ticker = None
        self._resetting_book = False
        self.actors = actors

    def _connect(self):
        super()._connect()

    def on_open(self):
        self._first_run = True
        Logger.info('gdax_order_book', "-- Subscribed to OrderBook! --\n")

    def on_close(self):
        Logger.info('gdax_order_book', "\n-- OrderBook Socket Closed! --")

    def _reset_bid_ask(self, product_id):
        self.books[product_id]["_asks"] = RBTree()
        self.books[product_id]["_bids"] = RBTree()

    def reset_product(self, product_id):
        res = self._client.get_product_order_book(product_id=product_id, level=3)
        self._reset_bid_ask(product_id)
        for bid in res['bids']:
            self.add(product_id, {
                'id': bid[2],
                'side': 'buy',
                'price': Decimal(bid[0]),
                'size': Decimal(bid[1])
            })
        for ask in res['asks']:
            self.add(product_id, {
                'id': ask[2],
                'side': 'sell',
                'price': Decimal(ask[0]),
                'size': Decimal(ask[1])
            })
        self.books[product_id]['sequence'] = res['sequence']

    def reset_book(self):
        Logger.info('gdax_order_book', "Resetting book")
        for prod in self.product_ids:
            self.reset_product(prod)

    def on_message(self, message):
        if self._first_run:
            self.reset_book()
            self._first_run = False

        sequence = message['sequence']
        product_id = message['product_id']
        product_sequence = self.books[product_id]['sequence']

        if sequence <= product_sequence:
            Logger.info('gdax_order_book', 'Older message: {}\nSequence:{}'.format(message, self.books[product_id]['sequence']))
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

        Logger.info('gdax_order_book',"Product: {}, Message: {}, book:{}" .format(product_id,sequence, self.books[product_id]['sequence']))
        self.books[product_id]['sequence'] = sequence
        self.send_book_to_subscribers()

    def send_book_to_subscribers(self):
        for actor_ref in self.actors:
            actor_ref.tell({'formatter': GDaxOrderBook.to_pandas_table,
                            'full_book':  self.books})

    def on_sequence_gap(self, gap_start, gap_end):
        Logger.info('gdax_order_book', 'Error: messages missing ({} - {}). Re-initializing  book at sequence.'
              .format(gap_start, gap_end))
        self.reset_book()

    def add(self, product_id, order):
        order = {
            'id': order.get('order_id') or order['id'],
            'side': order['side'],
            'price': Decimal(order['price']),
            'size': Decimal(order.get('size') or order['remaining_size'])
        }
        if order['side'] == 'buy':
            bids = self.get_bids(product_id, order['price'])
            if bids is None:
                bids = [order]
            else:
                bids.append(order)
            self.set_bids(product_id, order['price'], bids)
        else:
            asks = self.get_asks(product_id, order['price'])
            if asks is None:
                asks = [order]
            else:
                asks.append(order)
            self.set_asks(product_id, order['price'], asks)

    def remove(self, product_id, order):
        price = Decimal(order['price'])
        if order['side'] == 'buy':
            bids = self.get_bids(product_id, price)
            if bids is not None:
                bids = [o for o in bids if o['id'] != order['order_id']]
                if len(bids) > 0:
                    self.set_bids(product_id, price, bids)
                else:
                    self.remove_bids(product_id, price)
        else:
            asks = self.get_asks(product_id, price)
            if asks is not None:
                asks = [o for o in asks if o['id'] != order['order_id']]
                if len(asks) > 0:
                    self.set_asks(product_id, price, asks)
                else:
                    self.remove_asks(product_id, price)

    def match(self, product_id, order):
        size = Decimal(order['size'])
        price = Decimal(order['price'])

        if order['side'] == 'buy':
            bids = self.get_bids(product_id, price)
            if not bids:
                return
            assert bids[0]['id'] == order['maker_order_id']
            if bids[0]['size'] == size:
                self.set_bids(product_id, price, bids[1:])
            else:
                bids[0]['size'] -= size
                self.set_bids(product_id, price, bids)
        else:
            asks = self.get_asks(product_id, price)
            if not asks:
                return
            assert asks[0]['id'] == order['maker_order_id']
            if asks[0]['size'] == size:
                self.set_asks(product_id, price, asks[1:])
            else:
                asks[0]['size'] -= size
                self.set_asks(product_id, price, asks)

    def change(self, product_id, order):
        try:
            new_size = Decimal(order['new_size'])
        except KeyError:
            return

        try:
            price = Decimal(order['price'])
        except KeyError:
            return

        if order['side'] == 'buy':
            bids = self.get_bids(product_id, price)
            if bids is None or not any(o['id'] == order['order_id'] for o in bids):
                return
            index = [b['id'] for b in bids].index(order['order_id'])
            bids[index]['size'] = new_size
            self.set_bids(product_id, price, bids)
        else:
            asks = self.get_asks(product_id, price)
            if asks is None or not any(o['id'] == order['order_id'] for o in asks):
                return
            index = [a['id'] for a in asks].index(order['order_id'])
            asks[index]['size'] = new_size
            self.set_asks(product_id, price, asks)

        tree = self.books[product_id]['_asks'] if order['side'] == 'sell' else self.books[product_id]['_bids']
        node = tree.get(price)

        if node is None or not any(o['id'] == order['order_id'] for o in node):
            return

    def get_current_ticker(self):
        return self._current_ticker

    def get_ask(self, product_id):
        return self.books[product_id]['_asks'].min_key()

    def get_asks(self, product_id, price):
        return self.books[product_id]['_asks'].get(price)

    def remove_asks(self, product_id, price):
        self.books[product_id]['_asks'].remove(price)

    def set_asks(self, product_id, price, asks):
        self.books[product_id]['_asks'].insert(price, asks)

    def get_bid(self, product_id):
        return self.books[product_id]['_bids'].max_key()

    def get_bids(self, product_id, price):
        return self.books[product_id]['_bids'].get(price)

    def remove_bids(self, product_id, price):
        self.books[product_id]['_bids'].remove(price)

    def set_bids(self, product_id, price, bids):
        self.books[product_id]['_bids'].insert(price, bids)

    @staticmethod
    def get_product_book(book, product_id):
        result = {
            'sequence': book[product_id]['sequence'],
            'asks': [],
            'bids': [],
        }
        for ask in book[product_id]['_asks']:
            try:
                # There can be a race condition here, where a price point is removed
                # between these two ops
                this_ask = book[product_id]['_asks'][ask]
            except KeyError:
                continue
            for order in this_ask:
                result['asks'].append([order['side'], order['price'], order['size'], order['id']])
        for bid in book[product_id]['_bids']:
            try:
                # There can be a race condition here, where a price point is removed
                # between these two ops
                this_bid = book[product_id]['_bids'][bid]
            except KeyError:
                continue

            for order in this_bid:
                result['bids'].append([order['side'], order['price'], order['size'], order['id']])
        return result

    @staticmethod
    def get_full_book(book):
        res = {}
        for prod in book:
            res.update({prod: GDaxOrderBook.get_product_book(book, prod)})
        return res

    @staticmethod
    def to_pandas_table(current_book):
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
            super(OrderBookConsole, self).__init__(products=["BTC-USD", "ETH-USD"])

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
                    Logger.info('gdax_order_book', '{} {} bid: {:.3f} @ {:.2f}\task: {:.3f} @ {:.2f}'.format(
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
