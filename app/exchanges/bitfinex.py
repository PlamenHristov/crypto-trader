import logging
import sys
import zlib
from collections import Iterable
from decimal import Decimal
from functools import partial
from pprint import pprint as pp
from threading import Thread
import pandas as pd

from app.api.bitfinex_api import BitfinexREST
from app.exchanges.book import OrderBook

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

class BitfinexOrderBook(OrderBook):
    exchange_name = 'Bitfinex'

    def __init__(self, client=BitfinexREST(), url='wss://api.bitfinex.com/ws/2', *args, **kwargs):
        actors = kwargs.pop('actors', None)
        OrderBook.__init__(self, actors=actors, *args, **kwargs)
        self._client = client
        self.books = {
            prod: {
                "_asks": {},
                "_bids": {},
                "sequence": 0,
            }
            for prod in kwargs['products']
        }
        self.product_chanel_id = {}
        self.book_snap = {}

    def _get_order_book_for_product(self, product_id, level=3):
        return self._client.get_product_order_book(product_id=''.join(product_id.split('-')), level=level)

    def format_snapshot(self, product_id, data):
        self._reset_bid_ask(product_id)
        for snap_order in data:
            self.add(product_id, {
                'count': snap_order[1],
                'side': 'buy' if snap_order[2] > 0 else 'sell',
                'price': Decimal(snap_order[0]),
                'size': Decimal(snap_order[2]) if snap_order[2] > 0 else Decimal(abs(snap_order[2])),
            })

    def start(self):
        def _go():
            while True:
                self.reset_book()
                self.send_book_to_subscribers()

        self.stop = False
        self.thread = Thread(target=_go)
        self.thread.daemon = True
        self.thread.start()

    def on_message(self, message):
        self.send_book_to_subscribers()

    def convert_to_local(self, product):
        return product[:3] + '-' + product[3:]

    def reset_product(self, product_id):
        res = self._format_book_response(self._get_order_book_for_product(product_id=product_id))
        self._reset_bid_ask(product_id)
        try:
            for bid in res['bids']:
                self.add(product_id, {
                    'id': bid['timestamp'],
                    'side': 'buy',
                    'price': Decimal(bid['price']),
                    'size': Decimal(bid['amount'])
                })
            for ask in res['asks']:
                self.add(product_id, {
                    'id': ask['timestamp'],
                    'side': 'sell',
                    'price': Decimal(ask['price']),
                    'size': Decimal(ask['amount'])
                })
        except KeyError:
            print('WTF?!')

    def to_pandas_table(self, current_book):
        res_table = pd.DataFrame()

        for prod in current_book:
            bid_ask_data = current_book[prod]
            ask_tbl = pd.DataFrame(
                data=bid_ask_data['asks'],
                columns=['side', 'price', 'volume', 'count'],
                index=range(len(bid_ask_data['asks']))
            )
            bid_tbl = pd.DataFrame(
                data=bid_ask_data['bids'],
                columns=['side', 'price', 'volume', 'count'],
                index=range(len(bid_ask_data['bids']))
            )
            con_tbl = pd.concat([ask_tbl, bid_tbl, ], axis=0).reset_index(drop=True)
            con_tbl['ticker'] = prod
            res_table = pd.concat([con_tbl, res_table, ], axis=0).reset_index(drop=True)
        res_table['id'] = res_table.index
        return res_table
