import json
import logging
from decimal import Decimal, InvalidOperation
import math
from websocket import create_connection
from functools import partial
from collections import Iterable
from app.api.bitfinex_api import BitfinexREST
from app.api.websocket_client import WebsocketClient
from app.exchanges.book import OrderBook
from pprint import pprint as pp
import binascii
import sys

# url = "wss://ws-feed.gdax.com", products = None, message_type = "subscribe",
# should_print = True, auth = False, api_key = "", api_secret = "", api_passphrase = "", channels = None
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_handler = logging.StreamHandler(sys.stdout)  # Handler for the logger
logger.addHandler(logger_handler)


# Example update message structure [1765.2, 0, 1] where we have [price, count, amount].
# Update algorithm pseudocode from Bitfinex documentation:
# 1. - When count > 0 then you have to add or update the price level.
#   1.1- If amount > 0 then add/update bids.
#   1.2- If amount < 0 then add/update asks.
# 2. - When count = 0 then you have to delete the price level.
#   2.1- If amount = 1 then remove from bids
#   2.2- If amount = -1 then remove from asks

# Trading: if AMOUNT > 0 then bid else ask; Funding: if AMOUNT < 0 then bid else ask;

# Algorithm to create and keep a book instance updated
#
# subscribe to channel with R0 precision
# receive the raw book snapshot and create your in-memory book structure
# when PRICE > 0 then you have to add or update the order
# when PRICE = 0 then you have to delete the order
# Note: The algorithm for raw books using R0 is based on PRICE instead of COUNT and AMOUNT.

class BitfinexOrderBook(WebsocketClient, OrderBook):
    exchange_name = 'Bitfinex'

    def __init__(self, client=BitfinexREST(), url='wss://api.bitfinex.com/ws/2', *args, **kwargs):
        actors = kwargs.pop('actors', None)
        WebsocketClient.__init__(self, url=url, *args, **kwargs)
        OrderBook.__init__(self, actors=actors, *args, **kwargs)
        self._client = client
        self.product_chanel_id = {}
        self.book_snap = {}

    def _get_order_book_for_product(self, product_id, level=3):
        return self._client.get_product_order_book(product_id=''.join(product_id.split('-')), level=level)

    def format_snapshot(self, product_id, data):
        self._reset_bid_ask(product_id)
        for snap_order in data:
            self.add(product_id, {
                'id': snap_order[0],
                'side': 'buy' if snap_order[2] > 0 else 'sell',
                'price': Decimal(snap_order[1]),
                'size': Decimal(snap_order[2]) if snap_order[2] > 0 else Decimal(abs(snap_order[2])),
            })

    def on_open(self):
        self._first_run = True
        logger.info("-- Subscribed to OrderBook! --\n")

    def on_close(self):
        logger.info("\n-- OrderBook Socket Closed! --")

    def _connect(self):
        if self.product_ids is None:
            self.product_ids = ["BTC-USD"]
        elif not isinstance(self.product_ids, list):
            self.product_ids = [self.product_ids]

        sub_params = {
            "event": "subscribe",
            "channel": "book",
            "len": "25",
            "prec": "R0",
        }

        self.ws = create_connection(self.url)
        for prod in self.product_ids:
            sub_params["pair"] = 't' + ''.join(prod.split('-'))
            self.ws.send(json.dumps(sub_params))

        sub_params = {'event': 'conf', 'flags': 131072}
        self.ws.send(json.dumps(sub_params))

    def on_message(self, message):
        pp(message)

        if self.is_skipable_message(message):
            return
        #
        channelId = message[0]
        product = self.convert_to_local(self.product_chanel_id[channelId])
        data = message[1] if type(message[1]) is list else message

        # Update orderbook and filter out heartbeat messages.
        if data[1] == 'cs':
            self.validate_checksum(self.convert_to_local(self.product_chanel_id[message[0]]), message[2])
        elif data[1] != 'hb':
            # [order_id, price, amount].
            order = {'order_id': str(data[0]), 'price': Decimal(data[1]), 'size': Decimal(data[2])}

            if order['price'] > 0:  # 1.

                if order['size'] > 0:  # 1.1
                    order['side'] = 'buy'
                    order['size'] = abs(order['size'])
                    if not self.change(product, order):
                        self.add(product, order)

                elif order['size'] < 0:  # 1.2
                    order['side'] = 'sell'
                    order['size'] = abs(order['size'])
                    if not self.change(product, order):
                        self.add(product, order)

            elif order['price'] == 0:  # 2.

                if order['size'] == 1:  # 2.1
                    order['side'] = 'buy'
                    self.remove(product, order)

                elif order['size'] == -1:  # 2.2
                    order['side'] = 'sell'
                    order['size'] = abs(order['size'])
                    self.remove(product, order)

        self.save_snapshot(product)
        self.send_book_to_subscribers()

    def convert_to_local(self, product):
        return product[:3] + '-' + product[3:]

    def change(self, product_id, order):
        try:
            new_size = Decimal(order['size'])
        except KeyError:
            return False

        try:
            price = Decimal(order['price'])
        except KeyError:
            return False

        if order['side'] == 'buy':
            bids = self.get_bids(product_id, price)
            if bids is None or not any(o['id'] == order['order_id'] for o in bids):
                return False
            index = [b['id'] for b in bids].index(order['order_id'])
            bids[index]['size'] = new_size
            self.set_bids(product_id, price, bids)
        else:
            asks = self.get_asks(product_id, price)
            if asks is None or not any(o['id'] == order['order_id'] for o in asks):
                return False
            index = [a['id'] for a in asks].index(order['order_id'])
            asks[index]['size'] = new_size
            self.set_asks(product_id, price, asks)

        tree = self.books[product_id]['_asks'] if order['side'] == 'sell' else self.books[product_id]['_bids']
        node = tree.get(price)

        if node is None or not any(o['id'] == order['order_id'] for o in node):
            return False

        return True

    def remove(self, product_id, order):
        price = Decimal(order['price'])
        if order['side'] == 'buy':
            bids = self.get_bids(product_id, price)
            if bids is not None:
                bids = [o for o in bids if o['id'] != order['order_id']]
                if len(bids) > 0:
                    logger.info("Missing")
                    return
                    # self.set_bids(product_id, price, bids)
                else:
                    self.remove_bids(product_id, price)
        else:
            asks = self.get_asks(product_id, price)
            if asks is not None:
                asks = [o for o in asks if o['id'] != order['order_id']]
                if len(asks) > 0:
                    logger.info("Missing")
                    return
                    # self.set_asks(product_id, price, asks)
                else:
                    self.remove_asks(product_id, price)

    def save_snapshot(self, product_id):
        book = self.get_product_book(self.books, product_id)
        asks = sorted(book['asks'], key=lambda x: x[1])[:25]
        bids = sorted(book['bids'], key=lambda x: x[1], reverse=True)[:25]
        self.book_snap[product_id] = {'asks': asks, 'bids': bids}

    def is_skipable_message(self, message):
        if type(message) is dict:
            event = message.get('event', None)
            if event == 'info':
                return True
            elif event == 'subscribed':
                self.product_chanel_id[message['chanId']] = message['pair']
                return True
            elif event == 'conf':
                return True
            elif event == 'cs':
                self.validate_checksum(self.convert_to_local(self.product_chanel_id[message[0]]), message[2])
                return True
        # skip snapshot
        elif len(message[1]) > 10:
            self.format_snapshot(self.convert_to_local(self.product_chanel_id[message[0]]), message[1])
            return True
        return False

    @staticmethod
    def get_checksum_for_string(msg):
        return binascii.crc32(msg.encode('utf8'))

    def flatten(self, items):
        for x in items:
            if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
                yield from self.flatten(x)
            else:
                yield x

    def return_price_amount(self, x, is_negative=False):
        return str(x[1]), str(x[2]) if not is_negative else str(-x[2])

    def validate_checksum(self, product_id, checksum):
        csdata = []
        csdata.extend(list(self.flatten(map(self.return_price_amount, self.book_snap[product_id]['bids']))))
        csdata.extend(list(
            self.flatten(map(partial(self.return_price_amount, is_negative=True), self.book_snap[product_id]['asks']))))
        check_string = ':'.join(csdata)
        _checksum = self.get_checksum_for_string(check_string)
        if _checksum != checksum:
            logger.error("Different checksums")
        logger.error("{} - {}".format(_checksum, checksum))

    def reset_product(self, product_id):
        res = self._format_book_response(self._get_order_book_for_product(product_id=product_id))
        self._reset_bid_ask(product_id)
        for side in ['bids', 'asks']:
            for i, val in enumerate(res[side]):
                self.add(product_id, {
                    'id': i,
                    'price': Decimal(val['price']),
                    'size': Decimal(val['amount']),
                    'side': 'buy' if side is 'bids' else 'sell',
                    'timestamp': self.get_date_and_time(math.trunc(float(val['timestamp'])))
                })
