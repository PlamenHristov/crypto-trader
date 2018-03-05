from clients.sql import SqlClient
import pymysql
import pandas as pd
from util.logger import Logger


class MysqlClient(SqlClient):
    def __init__(self):
        SqlClient.__init__(self)

    def connect(self, **kwargs):
        host = kwargs['host']
        port = kwargs['port']
        user = kwargs['user']
        pwd = kwargs['pwd']
        schema = kwargs['schema']
        self.conn = pymysql.connect(host=host,
                                    port=port,
                                    user=user,
                                    password=pwd,
                                    db=schema,
                                    charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)

        self.cursor = self.conn.cursor()
        return self.conn is not None and self.cursor is not None

    def execute(self, sql):
        return self.cursor.execute(sql)

    def commit(self):
        self.conn.commit()

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def insert(self, table, columns, types, values, primary_key_index=(), is_orreplace=False, is_commit=True):
        try:
            if isinstance(values, pd.DataFrame):
                self.lock.acquire()
                values.to_sql(table,self.conn, index=False)
            else:
                super().insert(table, columns, types, values, primary_key_index, is_orreplace, is_commit)
        except Exception as e:
            Logger.info(self.__class__.__name__, "SQL error: %s\nSQL: %s" % (e, sql))
        self.lock.release()
        return True

    def select(self, table, columns=['*'], condition='', orderby='', limit=0, isFetchAll=True):
        select = SqlClient.select(self, table, columns, condition, orderby, limit, isFetchAll)
        if len(select) > 0:
            if columns[0] != '*':
                ret = []
                for ele in select:
                    row = []
                    for column in columns:
                        row.append(ele[column])

                    ret.append(row)
            else:
                ret = [list(e.values()) for e in select]

            return ret
        else:
            return select
