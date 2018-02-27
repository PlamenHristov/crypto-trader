def to_pandas_table(self, ):
    self.lock.aquire()
    try:
        asks_tbl = pd.DataFrame(data=self._asks, index=range(len(self._asks)))
        asks_tbl = pd.DataFrame(data=self._bids, index=range(len(self._bids)))
    finally:
        self.lock.release()
