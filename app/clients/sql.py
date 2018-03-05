from clients.database import DatabaseClient
from util.logger import Logger
import threading


class SqlClient(DatabaseClient):
    @classmethod
    def replace_keyword(cls):
        return 'replace into'

    def __init__(self):
        DatabaseClient.__init__(self)
        self.conn = None
        self.cursor = None
        self.lock = threading.Lock()

    def execute(self, sql):
        return True

    def commit(self):
        return True

    def fetchone(self):
        return []

    def fetchall(self):
        return []

    def create(self, table, columns, types, primary_key_index=(), is_ifnotexists=True):
        if len(columns) != len(types):
            raise Exception("Incorrect create statement. Number of columns and that of types are different.\n%s\n%s" % \
                            (columns, types))

        column_names = ''
        for i in range(0, len(columns)):
            column_names += '%s %s,' % (columns[i], types[i])

        if len(primary_key_index) > 0:
            column_names += 'PRIMARY KEY (%s)' % (",".join([columns[e] for e in primary_key_index]))
        else:
            column_names = column_names[0:len(column_names) - 1]

        if is_ifnotexists:
            sql = "create table if not exists %s (%s)" % (table, column_names)
        else:
            sql = "create table %s (%s)" % (table, column_names)

        self.lock.acquire()

        try:
            self.execute(sql)
        except Exception as e:
            raise Exception("Error in create statement (%s).\nError: %s\n" % (sql, e))

        self.commit()
        self.lock.release()
        return True

    def insert(self, table, columns, types, values, primary_key_index=(), is_orreplace=False, is_commit=True):
        if len(columns) != len(values):
            return False

        column_names = ','.join(columns)
        value_string = ','.join([SqlClient.convert_str(e) for e in values])
        if is_orreplace:
            sql = "%s %s (%s) values (%s)" % (self.replace_keyword(), table, column_names, value_string)
        else:
            sql = "insert into %s (%s) values (%s)" % (table, column_names, value_string)

        self.lock.acquire()
        try:
            self.execute(sql)
            if is_commit:
                self.commit()
        except Exception as e:
            Logger.info(self.__class__.__name__, "SQL error: %s\nSQL: %s" % (e, sql))
        self.lock.release()
        return True

    def select(self, table, columns=['*'], condition='', orderby='', limit=0, isFetchAll=True):
        sql = "select %s from %s" % (','.join(columns), table)
        if len(condition) > 0:
            sql += " where %s" % condition

        if len(orderby) > 0:
            sql += " order by %s" % orderby

        if limit > 0:
            sql += " limit %d" % limit

        self.lock.acquire()
        self.execute(sql)
        if isFetchAll:
            ret = self.fetchall()
            self.lock.release()
            return ret
        else:
            ret = self.fetchone()
            self.lock.release()
            return ret

    def delete(self, table, condition='1==1'):
        sql = "delete from %s" % table
        if len(condition) > 0:
            sql += " where %s" % condition

        self.lock.acquire()
        self.execute(sql)
        self.commit()
        self.lock.release()
        return True
