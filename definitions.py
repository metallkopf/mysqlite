from enum import IntFlag, IntEnum


class Capability(IntFlag):
  LONG_PASSWORD = 1 # new more secure passwords
  FOUND_ROWS = 2 # Found instead of affected rows
  LONG_FLAG = 4 # Get all column flags
  CONNECT_WITH_DB = 8 # One can specify db on connect
  NO_SCHEMA = 16 # Don't allow database.table.column
  COMPRESS = 32 # Can use compression protocol
  ODBC = 64 # Odbc client
  LOCAL_FILES = 128 # Can use LOAD DATA LOCAL
  IGNORE_SPACE = 256 # Ignore spaces before '('
  PROTOCOL_41 = 512 # New 4.1 protocol
  INTERACTIVE = 1024 # This is an interactive client
  SSL = 2048 # Switch to SSL after handshake
  IGNORE_SIGPIPE = 4096 # IGNORE sigpipes
  TRANSACTIONS = 8192 # Client knows about transactions
  RESERVED = 16384 # Old flag for 4.1 protocol
  SECURE_CONNECTION = 32768 # New 4.1 authentication
  MULTI_STATEMENTS = 65536 # Enable/disable multi-stmt support
  MULTI_RESULTS = 131072 # Enable/disable multi-results

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
  PREPARE = 22
  EXECUTE = 23
  LONG_DATA = 24
  CLOSE_STMT = 25
  RESET_STMT = 26
  SET_OPTION = 27
  END = 28

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
  VARCHAR = 0x0f
  BIT = 0x10
  NEWDECIMAL = 0xf6
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
  NUM = 32768 # Field is num (for clients)
