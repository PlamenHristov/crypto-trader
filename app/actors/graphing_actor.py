from collections import defaultdict

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib import style
from pykka import ThreadingActor

PRICE_RANGE = 0.3


class GraphingActor(ThreadingActor):
    subplots = None
    graphs = {}

    def __int__(self, *args, **kwargs):
        super(GraphingActor, self).__init__()
        self.init_graph()

    def init_graph(self):
        style.use('seaborn')
        plt.show(pause=False)

    @staticmethod
    def init_subplots(products):
        if GraphingActor.subplots is None:
            figs, GraphingActor.subplots = plt.subplots(len(products), 1, squeeze=False)
            for ax in GraphingActor.subplots.flat:
                ax.set(xlabel='Price', ylabel='Volume')

    def on_receive(self, message):
        full_book = self.get_full_book(message['full_book'])
        full_book_tbl = message['formatter'](full_book)
        price_vol_side_ticker = full_book_tbl[['price', 'volume', 'side', 'ticker']]
        products = price_vol_side_ticker.ticker.unique()

        GraphingActor.init_subplots(products)

        for i, prod in enumerate(products):
            filtered_product = price_vol_side_ticker.loc[price_vol_side_ticker['ticker'] == prod]
            ask_tbl = filtered_product.loc[filtered_product['side'] == 'sell']
            bid_tbl = filtered_product.loc[filtered_product['side'] == 'buy']

            ask_tbl = ask_tbl.sort_values(by='price', ascending=True)
            bid_tbl = bid_tbl.sort_values(by='price', ascending=False)

            # # get first on each side
            lowest_ask = float(ask_tbl['price'].iloc[0])
            highest_bid = float(bid_tbl['price'].iloc[0])

            # # get perc for ask/ bid
            perc_above_lowest_ask = ((1.0 + PRICE_RANGE) * lowest_ask)
            perc_above_highest_bid = ((1.0 - PRICE_RANGE) * highest_bid)
            #
            # # limits the size of the table so that we only look at orders 30% above and under market price
            ask_tbl = ask_tbl[(ask_tbl['price'] <= perc_above_lowest_ask)]
            bid_tbl = bid_tbl[(bid_tbl['price'] >= perc_above_highest_bid)]

            bid_tbl['volume_cumul'], ask_tbl['volume_cumul'] = bid_tbl['volume'].cumsum(), ask_tbl['volume'].cumsum()

            if not GraphingActor.graphs.get((prod, i, 0), None):
                GraphingActor.subplots[i, 0].set_title(prod)
                plot_bid = GraphingActor.subplots[i, 0].plot(bid_tbl['price'], bid_tbl['volume_cumul'], color='green',
                                                             linestyle='solid')[0]
                plot_ask = GraphingActor.subplots[i, 0].plot(ask_tbl['price'], ask_tbl['volume_cumul'], color='red',
                                                             linestyle='solid')[0]
                GraphingActor.subplots[i, 0].grid(True)
                GraphingActor.graphs.update({(prod, i, 0): [plot_bid, plot_ask]})
            else:
                plot_bid, plot_ask = GraphingActor.graphs[(prod, i, 0)]
                plot_bid.set_xdata(bid_tbl['price'])
                plot_bid.set_ydata(bid_tbl['volume_cumul'])
                plot_ask.set_xdata(ask_tbl['price'])
                plot_ask.set_ydata(ask_tbl['volume_cumul'])

            plt.draw()
            plt.pause(0.1)

    @staticmethod
    def get_product_book(book, product_id):
        result = {
            'sequence': book[product_id]['sequence'],
            'asks': [[order['side'], order['price'], order['size'], order['id']] for ask in book[product_id]['_asks']
                     for order in book[product_id]['_asks'][ask]],
            'bids': [[order['side'], order['price'], order['size'], order['id']] for bid in book[product_id]['_bids']
                     for order in book[product_id]['_bids'][bid]],
        }
        return result

    @staticmethod
    def get_full_book(book):
        res = {}
        for prod in book:
            res.update({prod: GraphingActor.get_product_book(book, prod)})
        return res
