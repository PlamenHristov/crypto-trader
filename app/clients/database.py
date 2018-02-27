class DatabaseClient:
    def __init__(self):
        pass

    @classmethod
    def convert_str(cls, val):
        if isinstance(val, str):
            return "'" + val + "'"
        elif isinstance(val, bytes):
            return "'" + str(val) + "'"
        elif isinstance(val, int):
            return str(val)
        elif isinstance(val, float):
            return "%.8f" % val
        else:
            raise Exception("Cannot convert value (%s)<%s> to string. Value is not a string, an integer nor a float" % \
                            (val, type(val)))

    def connect(self, **args):
        return True

    def create(self, table, columns, types, primary_key_index=(), is_ifnotexists=True):
        return True

    def insert(self, table, columns, types, values, primary_key_index=(), is_orreplace=False, is_commit=True):
        return True

    def select(self, table, columns=['*'], condition='', orderby='', limit=0, isFetchAll=True):
        return True

    def close(self):
        return True
