import datetime as dt
from pykka import ThreadingActor


class GraphingActor(ThreadingActor):
    def __int__(self, *args, **kwargs):
        super(GraphingActor, self).__init__()
        self._bid = None
        self._ask = None
        self._bid_depth = None
        self._ask_depth = None

    def on_receive(self, message):
        full_book = message['full_book']
        for product_id in full_book:
            # Calculate newest bid-ask spread
            bid = self.get_bid(full_book[product_id], )
            bids = self.get_bids(full_book[product_id], bid)
            bid_depth = sum([b['size'] for b in bids])
            ask = self.get_ask(full_book[product_id], )
            asks = self.get_asks(full_book[product_id], ask)
            ask_depth = sum([a['size'] for a in asks])


            # If there are differences, update the cache
            self._bid = bid
            self._ask = ask
            self._bid_depth = bid_depth
            self._ask_depth = ask_depth
            print('{} {} bid: {:.3f} @ {:.2f}\task: {:.3f} @ {:.2f}'.format(
                dt.datetime.now(), product_id, bid_depth, bid, ask_depth, ask))

    def get_ask(self, product_book):
        return product_book['_asks'].min_key()

    def get_asks(self, product_book, price):
        return product_book['_asks'].get(price)

    def get_bid(self, product_book):
        return product_book['_bids'].max_key()

    def get_bids(self, product_book, price):
        return product_book['_bids'].get(price)
