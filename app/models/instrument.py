class Instrument:
    def __init__(self,
                 exchange_name,
                 instmt_name,
                 instmt_code,
                 **param):
        self.exchange_name = exchange_name
        self.instmt_name = instmt_name
        self.instmt_code = instmt_code
        self.instmt_snapshot_table_name = ''

    def get_exchange_name(self):
        return self.exchange_name

    def get_instmt_name(self):
        return self.instmt_name

    def get_instmt_code(self):
        return self.instmt_code

    def get_instmt_snapshot_table_name(self):
        return self.instmt_snapshot_table_name

    def set_instmt_snapshot_table_name(self, instmt_snapshot_table_name):
        self.instmt_snapshot_table_name = instmt_snapshot_table_name