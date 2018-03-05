from __future__ import print_function

import logging

from app.exchanges.book import OrderBook
from bittrex_websocket.websocket_client import BittrexSocket
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_handler = logging.StreamHandler()  # Handler for the logger
logger.addHandler(logger_handler)


class BittrexOrderBook(BittrexSocket, OrderBook):
    exchange_name = 'Bittrex'

    def __init__(self, *args, **kwargs):
        actors = kwargs.pop('actors', None)
        BittrexSocket.__init__(self, )
        OrderBook.__init__(self, actors=actors, *args, **kwargs)

    def on_orderbook_update(self, msg):
        print('[OrderBook]: {}'.format(msg['MarketName']))

    def on_orderbook(self, msg):
        self.books[msg['MarketName']] = {'_bids': msg['Buys'], '_asks': msg['Sells']}
        self.send_book_to_subscribers()

    def start(self):
        self.subscribe_to_orderbook(self.product_ids, book_depth=2000)

    def get_full_book(self, book):
        res = {}
        for prod in book:
            res.update({prod: self.get_product_book(book, prod)})
        return res

    def get_product_book(self, book, prod):
        result = {
            'asks': [['sell', order['Rate'], order['Quantity']] for order in book[prod]['_asks']],
            'bids': [['buy', order['Rate'], order['Quantity']] for order in book[prod]['_bids']],
        }
        return result

    def close(self):
        pass

    def to_pandas_table(self, current_book):
        res_table = pd.DataFrame()

        for prod in current_book:
            bid_ask_data = current_book[prod]
            ask_tbl = pd.DataFrame(
                data=bid_ask_data['asks'],
                columns=['side', 'price', 'volume'],
                index=range(len(bid_ask_data['asks']))
            )
            bid_tbl = pd.DataFrame(
                data=bid_ask_data['bids'],
                columns=['side', 'price', 'volume'],
                index=range(len(bid_ask_data['bids']))
            )
            con_tbl = pd.concat([ask_tbl, bid_tbl, ], axis=0).reset_index(drop=True)
            con_tbl['ticker'] = prod
            res_table = pd.concat([con_tbl, res_table, ], axis=0).reset_index(drop=True)
        res_table['id'] = res_table.index
        return res_table
