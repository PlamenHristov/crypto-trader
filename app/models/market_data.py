#!/bin/python


class Snapshot:
    class UpdateType:
        NONE = 0
        ORDER_BOOK = 1
        TRADES = 2

    def __init__(self):
        pass

    @staticmethod
    def snapshot_columns():
        return ['exchange', 'instmt',
                'bid', 'bid_quantity', 'ask', 'ask_quantity',
                'order_date_time', 'trades_date_time']

    @staticmethod
    def snapshot_types(is_name=True):
        """
        Return static column types
        """
        return ['varchar(20)', 'varchar(20)'] + \
               ['decimal(20,8)'] * 4 + \
               ['varchar(25)', 'varchar(25)']

    @staticmethod
    def inst_columns():
        return [
            'snapshot_id', 'type', 'price', 'volume', 'quantity',
        ]

    @staticmethod
    def inst_types():
        return ['int', 'varchar(20)', 'decimal(20,8)', 'decimal(20,8)', 'decimal(20,8)']
