import logging
from decimal import Decimal
from functools import partial
import time
import pandas as pd

from bintrees import RBTree

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_handler = logging.StreamHandler()  # Handler for the logger
logger.addHandler(logger_handler)


class OrderBook(object):
    exchange_name = None

    def __init__(self, products=list('BTC-USD'), actors=list()):
        self.books = {
            prod: {
                "_asks": RBTree(),
                "_bids": RBTree(),
                "sequence": 0,
            }
            for prod in products
        }
        self.product_ids = products
        self._first_run = True
        self.actors = actors
        self._current_ticker = None

    def _reset_bid_ask(self, product_id):
        self.books[product_id]["_asks"] = RBTree()
        self.books[product_id]["_bids"] = RBTree()

    def _get_order_book_for_product(self, product_id, level=3):
        raise NotImplementedError("Should be overriden")

    def _format_book_response(self, book_resp):
        return book_resp

    def reset_product(self, product_id):
        res = self._format_book_response(self._get_order_book_for_product(product_id=product_id))
        self._reset_bid_ask(product_id)
        try:
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
        except KeyError:
            print('WTF?!')

    def reset_book(self):
        for prod in self.product_ids:
            self.reset_product(prod)

    def send_book_to_subscribers(self):
        print(self.exchange_name)
        for actor_ref in self.actors:
            actor_ref.tell({'formatter': self.to_pandas_table,
                            'full_book': partial(self.get_full_book, self.books),
                            'exchange': self.exchange_name})

    def on_sequence_gap(self, gap_start, gap_end):
        logger.info('Error: messages missing ({} - {}). Re-initializing  book at sequence.'
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

    def get_product_book(self, book, product_id):
        result = {
            'sequence': book[product_id].get('sequence', None),
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
                result['asks'].append(
                    [order['side'], order['price'], order['size'], order.get('id', None) or order.get('count', None)])
        for bid in book[product_id]['_bids']:
            try:
                # There can be a race condition here, where a price point is removed
                # between these two ops
                this_bid = book[product_id]['_bids'][bid]
            except KeyError:
                continue

            for order in this_bid:
                result['bids'].append([order['side'], order['price'], order['size'], order['id'] or order['count']])
        return result

    def get_full_book(self, book):
        res = {}
        for prod in book:
            res.update({prod: self.get_product_book(book, prod)})
        return res

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

    def get_date_and_time(self, seconds=None):
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(seconds))
