import datetime
import threading
from functools import partial

import numpy as np
import pandas as pd

from app.models.market_data import Snapshot
from app.util.logger import Logger
from retired.gateway import ExchangeCoordinator

TBL_PRICE = 'price'
TBL_VOLUME = 'volume'
TBL_ORDER_ID = 'order_id'


class ExchGwApiGdaxOrderBook:
    def __init__(self, price_range=0.3):
        self.price_range = price_range

    @staticmethod
    def get_bids_field_name():
        return 'bids'

    @staticmethod
    def get_asks_field_name():
        return 'asks'

    def parse_l3_depth(self, order_book):
        if not order_book:
            return order_book

        ask_tbl = pd.DataFrame(
            data=order_book[self.get_asks_field_name()],
            columns=[TBL_PRICE, TBL_VOLUME, TBL_ORDER_ID],
            index=range(len(order_book[self.get_asks_field_name()]))
        )
        bid_tbl = pd.DataFrame(
            data=order_book[self.get_bids_field_name()],
            columns=[TBL_PRICE, TBL_VOLUME, TBL_ORDER_ID],
            index=range(len(order_book[self.get_bids_field_name()]))
        )
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d %H:%M:%S.%f")
        ask_tbl[TBL_PRICE] = pd.to_numeric(ask_tbl[TBL_PRICE])
        bid_tbl[TBL_PRICE] = pd.to_numeric(bid_tbl[TBL_PRICE])
        ask_tbl = ask_tbl.sort_values(by=TBL_PRICE, ascending=True)
        bid_tbl = bid_tbl.sort_values(by=TBL_PRICE, ascending=False)
        # get first on each side
        lowest_ask = float(ask_tbl.iloc[1, 0])
        highest_bid = float(bid_tbl.iloc[1, 0])
        # get perc for ask/ bid
        perc_above_lowest_ask = ((1.0 + self.price_range) * lowest_ask)
        perc_above_highest_bid = ((1.0 - self.price_range) * highest_bid)

        # limits the size of the table so that we only look at orders 30% above and under market price
        ask_tbl = ask_tbl[(ask_tbl[TBL_PRICE] <= perc_above_lowest_ask)]
        bid_tbl = bid_tbl[(bid_tbl[TBL_PRICE] >= perc_above_highest_bid)]

        # limits the size of the table so that we only look at orders 30% above and under market price
        ask_tbl.insert(0, 'type', ['ask'] * ask_tbl.shape[0])
        bid_tbl.insert(0, 'type', ['bid'] * bid_tbl.shape[0])

        con_tbl = pd.concat([ask_tbl, bid_tbl, ], axis=0).reset_index(drop=True)
        additional_data = pd.DataFrame(
            np.array([[1, timestamp, Snapshot.UpdateType.ORDER_BOOK] for _ in range(con_tbl.shape[0])]),
            columns=['quantity', 'order_date_time', 'update_type'], index=range(con_tbl.shape[0]))
        con_tbl = pd.concat([con_tbl, additional_data, ], axis=1)
        con_tbl = con_tbl.drop(['order_id'], axis=1)
        con_tbl['id'] = con_tbl.index
        return con_tbl

    def parse_trade(self, instmt, raw):
        raise Exception("parse_trade should not be called.")

    def get_order_book(self, instmt):
        order_book = self.request(self.get_order_book_link(instmt))
        if len(order_book) > 0:
            return self.parse_l3_depth(order_book=order_book)
        else:
            return None


class ExchGwGdax(ExchangeCoordinator):
    def __init__(self, db_clients):
        super().__init__(api_socket=ExchGwApiGdaxOrderBook(), db_clients=db_clients)

    @classmethod
    def get_exchange_name(cls):
        return 'Gdax'

    def get_order_book_worker(self, instmt):
        while True:
            try:
                con_tbl = self.api_socket.get_order_book(instmt=instmt)
                self.insert_order_book(con_tbl)

            except Exception as e:
                Logger.error(self.__class__.__name__, "Error in order book: %s" % e)

    def start(self, instmt):
        instmt.set_instmt_snapshot_table_name(
            self.get_instmt_snapshot_table_name(
                instmt.get_exchange_name(),
                instmt.get_instmt_name()
            )
        )
        self.init_instmt_snapshot_table(instmt)

        t_order_book = threading.Thread(target=partial(self.get_order_book_worker, instmt))
        t_order_book.start()

        return [t_order_book]
