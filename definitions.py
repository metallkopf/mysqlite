from enum import IntFlag, IntEnum


class Capability(IntFlag):
  LONG_PASSWORD = 0x1 # new more secure passwords
  FOUND_ROWS = 0x2 # Found instead of affected rows
  LONG_FLAG = 0x4 # Get all column flags
  CONNECT_WITH_DB = 0x8 # One can specify db on connect
  NO_SCHEMA = 0x10 # Don't allow database.table.column
  COMPRESS = 0x20 # Can use compression protocol
  PROTOCOL_41 = 0x200 # New 4.1 protocol
  INTERACTIVE = 0x400 # This is an interactive client
  TRANSACTIONS = 0x2000 # Client knows about transactions
  SECURE_CONNECTION = 0x8000 # New 4.1 authentication
  CONNECT_ATTRS = 0x100000 # Supports connection attributes

class Command(IntEnum):
  SLEEP = 0
  QUIT = 1
  INIT_DB = 2
  QUERY = 3
  FIELD_LIST = 4
  CREATE_DB = 5
  DROP_DB = 6
  REFRESH = 7
  SHUTDOWN = 8
  STATISTICS = 9
  PROCESS_INFO = 10
  CONNECT = 11
  PROCESS_KILL = 12
  DEBUG = 13
  PING = 14
  TIME = 15
  DELAYED_INSERT = 16
  CHANGE_USER = 17
  BINLOG_DUMP = 18
  TABLE_DUMP = 19
  CONNECT_OUT = 20
  REGISTER_SLAVE = 21
  STMT_PREPARE = 22
  STMT_EXECUTE = 23
  STMT_SEND_LONG_DATA = 24
  STMT_CLOSE = 25
  STMT_RESET = 26
  SET_OPTION = 27
  STMT_FETCH = 28

class Status(IntFlag):
  IN_TRANS = 1 # Transaction has started
  AUTOCOMMIT = 2 # Server in auto_commit mode
  MORE_RESULTS = 4 # More results on server
  MORE_RESULTS_EXISTS = 8 # Multi query - next query exists
  QUERY_NO_GOOD_INDEX_USED = 16
  QUERY_NO_INDEX_USED = 32
  DB_DROPPED = 256 # A database was dropped

class Charset(IntEnum):
  UTF8_GENERAL_CI = 33
  BINARY = 63

class FieldType(IntEnum):
  DECIMAL = 0x00
  TINY = 0x01
  SHORT = 0x02
  LONG = 0x03
  FLOAT = 0x04
  DOUBLE = 0x05
  NULL = 0x06
  TIMESTAMP = 0x07
  LONGLONG = 0x08
  INT24 = 0x09
  DATE = 0x0a
  TIME = 0x0b
  DATETIME = 0x0c
  YEAR = 0x0d
  NEWDATE = 0x0e
  ENUM = 0xf7
  SET = 0xf8
  TINY_BLOB = 0xf9
  MEDIUM_BLOB = 0xfa
  LONG_BLOB = 0xfb
  BLOB = 0xfc
  VAR_STRING = 0xfd
  STRING = 0xfe
  GEOMETRY = 0xff

class FieldFlag(IntFlag):
  NOT_NULL = 1 # Field can't be NULL
  PRI_KEY = 2 # Field is part of a primary key
  UNIQUE_KEY = 4 # Field is part of a unique key
  MULTIPLE_KEY = 8 # Field is part of a key
  BLOB = 16 # Field is a blob
  UNSIGNED = 32 # Field is unsigned
  ZEROFILL = 64 # Field is zerofill
  BINARY = 128 # Field is binary
  ENUM = 256 # field is an enum
  AUTO_INCREMENT = 512 # field is a autoincrement field
  TIMESTAMP = 1024 # Field is a timestamp
  SET = 2048 # field is a set
