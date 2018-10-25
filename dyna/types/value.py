from ..dynagatewaytypes.general_types_pb2 import Value as PBValue
from ..dynagatewaytypes.enums_pb2 import DataType
from ..dynagatewaytypes.enums_pb2 import INTEGER, STRING, BOOLEAN, DECIMAL, TIMESTAMP, URI, VOID
from ..dynagatewaytypes.enums_pb2 import FLOAT, UNSIGNED_INTEGER, BINARY, UUID
from ..dynagatewaytypes.datatypes_pb2 import Decimal as PBDecimal
from ..dynagatewaytypes.datatypes_pb2 import Uri as PBUri
from ..dynagatewaytypes.datatypes_pb2 import Timestamp as PBTimestamp
from ..common.errors import DatatypeError, DynaValueError
import uuid

class Decimal:
    def __init__(self, coefficient: int, exponent: int):
        self.coefficient = coefficient
        self.exponent = exponent

    def __str__(self):
        return '{0}E{1}'.format(self.coefficient, self.exponent)

    @staticmethod
    def _from_pb(decimal: PBDecimal):
        return Decimal(decimal.coefficient, decimal.exponent)

    def _to_pb(self) -> PBDecimal:
        pb = PBDecimal()
        pb.coefficient = self.coefficient
        pb.exponent = self.exponent
        return pb


class Uri:
    def __init__(self, schema: str, user_name: str, user_password: str,
                       host: str, port: int, path: str, query: str, fragment: str):
        self.schema = schema
        self.user_name = user_name
        self.user_password = user_password
        self.host = host
        self.port = port
        self.path = path
        self.query = query
        self.fragment = fragment

    def __str__(self):
        val = '{0}://'.format(self.schema)
        if self.user_name != "" and self.user_password != "":
            val = '{0}{1}:{2}@'.format(val, self.user_name, self.user_password)
        val = '{0}{1}'.format(val, self.host)
        if self.port != 0:
            val = '{0}:{1}'.format(val, self.port)
        path = '/'
        if self.path != "":
            if self.path[0] == '/':
                path = self.path
            else:
                path += self.path
        val = '{0}{1}'.format(val, path)
        if self.query != "":
            val = '{0}{1}'.format(val, self.query)
        if self.fragment != "":
            val = '{0}{1}'.format(val, self.fragment)
        return val


    @staticmethod
    def _form_pb(uri: PBUri):
        return Uri(uri.schema, uri.user_name, uri.user_password, uri.host, uri.port,
                    uri.path, uri.query, uri.fragment)

    def _to_pb(self) -> PBUri:
        pb = PBUri()
        pb.schema = self.schema
        pb.user_name = self.user_name
        pb.user_password = self.user_password
        pb.host = self.host
        pb.port = self.port
        pb.path = self.path
        pb.query = self.query
        pb.fragment = self.fragment
        return pb


class Timestamp:
    def __init__(self, unix_seconds: int, timezone: str, offset: int):
        self.unix_seconds = unix_seconds
        self.timezone = timezone
        self.offset = offset

    def __str__(self):
        val = str(self.unix_seconds)

        if self.offset < 0:
            val = '{0} {1}'.format(val, self.offset)
        elif self.offset > 0:
            val = '{0} +{1}'.format(val, self.offset)

        if self.timezone != "":
            val = '{0} {1}'.format(val, self.timezone)
        return val


    @staticmethod
    def _from_pb(ts: PBTimestamp):
        return Timestamp(ts.unix_seconds, ts.timezone, ts.offset)
        
    def _to_pb(self) -> PBTimestamp:
        pb = PBTimestamp()
        pb.unix_seconds = self.unix_seconds
        pb.timezone = self.timezone
        pb.offset = self.offset
        return pb


class Value:
    def __init__(self, data_type: DataType, value):
        if data_type == INTEGER:
            self.__init_integer(value)
        elif data_type == STRING:
            self.__init_string(value)
        elif data_type == BOOLEAN:
            self.__init_boolean(value)
        elif data_type == DECIMAL:
            self.__init_decimal(value)
        elif data_type == TIMESTAMP:
            self.__init_timestamp(value)
        elif data_type == URI:
            self.__init_uri(value)
        elif data_type == VOID:
            self.__init_void(value)
        elif data_type == FLOAT:
            self.__init_float(value)
        elif data_type == UNSIGNED_INTEGER:
            self.__init_unsigned_integer(value)
        elif data_type == BINARY:
            self.__init_binary(value)
        elif data_type == UUID:
            self.__init_uuid(value)
        else:
            raise DatatypeError(data_type)

        self.data_type = data_type

    def __str__(self):
        return str(self.value)

    def __init_integer(self, value):
        if not isinstance(value, int):
            if isinstance(value, str):
                try:
                    self.value = int(value)
                except Exception as e:
                    raise DynaValueError(value, 'INTEGER') from e
            else:
                raise DynaValueError(value, 'INTEGER')
        else:
            self.value = value

    def __init_string(self, value):
        if not isinstance(value, str):
            self.value = str(value)
        else:
            self.value = value

    def __init_boolean(self, value):
        if not isinstance(value, bool):
            if isinstance(value, int):
                self.value = value != 0
            elif isinstance(value, str):
                self.value = value.lower() == 'true'
            else:
                raise DynaValueError(value, 'BOOLEAN')
        else:
            self.value = value

    def __init_decimal(self, value):
        if not isinstance(value, Decimal):
            raise DynaValueError(value, 'DECIMAL')
        self.value = value

    def __init_timestamp(self, value):
        if not isinstance(value, Timestamp):
            raise DynaValueError(value, 'TIMESTAMP')
        self.value = value

    def __init_uri(self, value):
        if not isinstance(value, Uri):
            raise DynaValueError(value, 'URI')
        self.value = value

    def __init_void(self, value):
        self.value = None

    def __init_float(self, value):
        if not isinstance(value, float):
            if isinstance(value, int):
                self.value = float(value)
            elif isinstance(value, str):
                try:
                    self.value = float(value)
                except Exception as e:
                    raise DynaValueError(value, 'FLOAT') from e
            else:
                raise DynaValueError(value, 'FLOAT')
        else:
            self.value = value
        
    def __init_unsigned_integer(self, value):
        if not isinstance(value, int):
            if isinstance(value, str):
                val = 0
                try:
                    val = int(value)
                except Exception as e:
                    raise DynaValueError(value, "UNSIGNED_INTEGER") from e
                if val < 0:
                    raise DynaValueError(value, 'UNSIGNED_INTEGER')
                self.value = val
            else:
                raise DynaValueError(value, 'UNSIGNED_INTEGER')
        elif value < 0:
            raise DynaValueError(value, 'UNSIGNED_INTEGER')
        else:
            self.value = value

    def __init_binary(self, value):
        if not isinstance(value, bytes):
            raise DynaValueError(value, 'BINARY')
        self.value = value

    def __init_uuid(self, value):
        if not isinstance(uuid.UUID):
            raise DynaValueError(value, 'UUID')
        self.value = str(value)


    @staticmethod
    def _from_pb(v: PBValue):
        val = None
        if v.data_type == INTEGER:
            val = v.integer_value
        elif v.data_type == STRING:
            val = v.string_value
        elif v.data_type == BOOLEAN:
            val = v.boolean_value
        elif v.data_type == DECIMAL:
            val = Decimal._from_pb(v.decimal_value)
        elif v.data_type == TIMESTAMP:
            val = Timestamp._from_pb(v.timestamp_value)
        elif v.data_type == URI:
            val = Uri._form_pb(v.uri_value)
        elif v.data_type == FLOAT:
            val = v.float_value
        elif v.data_type == UNSIGNED_INTEGER:
            val = v.unsinged_integer_value
        elif v.data_type == BINARY:
            val = v.binary_value
        elif v.data_type == UUID:
            val = uuid.UUID(v.uuid_value)

        return Value(v.data_type, val)

    @staticmethod
    def _value_to_pb(v) -> PBValue:
        return v._to_pb()

    def _to_pb(self) -> PBValue:
        pb = PBValue()
        pb.data_type = self.data_type

        if self.data_type == INTEGER:
            pb.integer_value = self.value
        elif self.data_type == STRING:
            pb.string_value = self.value
        elif self.data_type == BOOLEAN:
            pb.boolean_value = self.value
        elif self.data_type == DECIMAL:
            pb.decimal_value = self.value._to_pb()
        elif self.data_type == TIMESTAMP:
            pb.timestamp_value = self.value._to_pb()
        elif self.data_type == URI:
            pb.uri_value = self.value._to_pb()
        elif self.data_type == FLOAT:
            pb.float_value = self.value
        elif self.data_type == UNSIGNED_INTEGER:
            pb.unsinged_integer_value = self.value
        elif self.data_type == BINARY:
            pb.binary_value = self.value
        elif self.data_type == UUID:
            pb.uuid_value = str(self.value)
        
        return pb

