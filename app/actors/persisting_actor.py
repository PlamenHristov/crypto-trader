from app.actors.actor import Actor
import datetime
from threading import Lock
from app.models.market_data import Snapshot


class PersistingActor(Actor):
    def __init__(self, db_clients=list()):
        self.db_clients = db_clients
        self.lock = Lock()
        self.exch_snapshot_id = None
        self.date_time = datetime.datetime.utcnow().date()

    @classmethod
    def get_exchange_name(cls):
        return ''

    def get_instmt_snapshot_table_name(self, exchange, instmt_name):
        return 'exch_' + exchange.lower() + '_' + instmt_name.lower() + \
               '_snapshot_' + self.date_time.strftime("%Y%m%d")

    @classmethod
    def is_allowed_instmt_record(cls, db_client):
        return not isinstance(db_client, ZmqClient)

    def init_instmt_snapshot_table(self, instmt):
        table_name = self.get_instmt_snapshot_table_name(instmt.get_exchange_name(),
                                                         instmt.get_instmt_name())

        instmt.set_instmt_snapshot_table_name(table_name)

        for db_client in self.db_clients:
            db_client.create(table_name,
                             ['id'] + Snapshot.inst_columns(),
                             ['int'] + Snapshot.inst_types(),
                             [0], is_ifnotexists=True)

            if isinstance(db_client, MysqlClient):
                with self.lock:
                    r = db_client.execute('select max(snapshot_id) from {};'.format(table_name))
                    db_client.conn.commit()
                    if r:
                        res = db_client.cursor.fetchone()
                        max_id = res['max(snapshot_id)'] if isinstance(db_client, MysqlClient) else res[0]
                        if max_id:
                            self.exch_snapshot_id = max_id
                        else:
                            self.exch_snapshot_id = 0

    def get_instmt_snapshot_id(self):
        with self.lock:
            self.exch_snapshot_id += 1

        return self.exch_snapshot_id

    def insert_order_book(self, instmt):
        # Update the snapshot
        if instmt is not None:
            for db_client in self.db_clients:
                if self.is_allowed_instmt_record(db_client):
                    db_client.insert(instmt)

    @staticmethod
    def seconds():
        return int(time.time())

    @staticmethod
    def milliseconds():
        return int(time.time() * 1000)

    @staticmethod
    def seconds():
        return int(time.time())

    @staticmethod
    def milliseconds():
        return int(time.time() * 1000)

    @staticmethod
    def microseconds():
        return int(time.time() * 1000000)

    @staticmethod
    def iso8601(timestamp):
        if timestamp is None:
            return timestamp
        utc = datetime.datetime.utcfromtimestamp(int(round(timestamp / 1000)))
        return utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-6] + "{:<03d}".format(int(timestamp) % 1000) + 'Z'

    @staticmethod
    def ymd(timestamp):
        utc_datetime = datetime.datetime.utcfromtimestamp(int(round(timestamp / 1000)))
        return utc_datetime.strftime('%Y-%m-%d')

    @staticmethod
    def ymdhms(timestamp, infix=' '):
        utc_datetime = datetime.datetime.utcfromtimestamp(int(round(timestamp / 1000)))
        return utc_datetime.strftime('%Y-%m-%d' + infix + '%H:%M:%S')

    @staticmethod
    def parse8601(timestamp):
        yyyy = '([0-9]{4})-?'
        mm = '([0-9]{2})-?'
        dd = '([0-9]{2})(?:T|[\s])?'
        h = '([0-9]{2}):?'
        m = '([0-9]{2}):?'
        s = '([0-9]{2})'
        ms = '(\.[0-9]{1,3})?'
        tz = '(?:(\+|\-)([0-9]{2})\:?([0-9]{2})|Z)?'
        regex = r'' + yyyy + mm + dd + h + m + s + ms + tz
        match = re.search(regex, timestamp, re.IGNORECASE)
        yyyy, mm, dd, h, m, s, ms, sign, hours, minutes = match.groups()
        ms = ms or '.000'
        msint = int(ms[1:])
        sign = sign or ''
        sign = int(sign + '1')
        hours = int(hours or 0) * sign
        minutes = int(minutes or 0) * sign
        offset = datetime.timedelta(hours=hours, minutes=minutes)
        string = yyyy + mm + dd + h + m + s + ms + 'Z'
        dt = datetime.datetime.strptime(string, "%Y%m%d%H%M%S.%fZ")
        dt = dt + offset
        return calendar.timegm(dt.utctimetuple()) * 1000 + msint
