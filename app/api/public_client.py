#
# GDAX/PublicClient.py
# Daniel Paquin
#
# For public requests to the GDAX exchange

import requests
import time


class PublicClient(object):
    def __init__(self, api_url='https://api.gdax.com', timeout=30):
        self.url = api_url.rstrip('/')
        self.timeout = timeout
        self.lastRestRequestTimestamp = 0
        self.rateLimit = 400
        self.enableRateLimit = True

    def _get(self, path, params=None):
        if self.enableRateLimit:
            self.throttle()
        self.lastRestRequestTimestamp = self.milliseconds()
        r = requests.get(self.url + path, params=params, timeout=self.timeout)
        # r.raise_for_status()
        return r.json()

    def get_products(self):
        return self._get('/products')

    def get_product_order_book(self, product_id, level=1):
        return self._get('/products/{}/book'.format(str(product_id)), params={'level': level})

    def get_product_ticker(self, product_id):
        return self._get('/products/{}/ticker'.format(str(product_id)))

    def get_product_trades(self, product_id):
        return self._get('/products/{}/trades'.format(str(product_id)))

    def get_product_historic_rates(self, product_id, start=None, end=None,
                                   granularity=None):
        params = {}
        if start is not None:
            params['start'] = start
        if end is not None:
            params['end'] = end
        if granularity is not None:
            acceptedGrans = [60, 300, 900, 3600, 21600, 86400]
            if granularity not in acceptedGrans:
                newGranularity = min(acceptedGrans, key=lambda x: abs(x - granularity))
                print(granularity, ' is not a valid granularity level, using', newGranularity, ' instead.')
                granularity = newGranularity
            params['granularity'] = granularity

        return self._get('/products/{}/candles'.format(str(product_id)), params=params)

    def get_product_24hr_stats(self, product_id):
        return self._get('/products/{}/stats'.format(str(product_id)))

    def get_currencies(self):
        return self._get('/currencies')

    def get_time(self):
        return self._get('/time')

    def throttle(self):
        now = float(self.milliseconds())
        elapsed = now - self.lastRestRequestTimestamp
        if elapsed < self.rateLimit:
            delay = self.rateLimit - elapsed
            time.sleep(delay / 1000.0)

    def milliseconds(self):
        return int(time.time() * 1000)
