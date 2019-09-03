#!/usr/bin/env python

from socketserver import StreamRequestHandler
from threading import current_thread
from binascii import hexlify
from io import BytesIO
from database import Database
from sqlparse import parse
from definitions import *
from logging import debug, info, warning, error
from traceback import print_exc
from utils import *


CAPABILITIES = Capability.LONG_PASSWORD | Capability.FOUND_ROWS | \
  Capability.LONG_FLAG | Capability.CONNECT_WITH_DB | Capability.NO_SCHEMA | \
  Capability.PROTOCOL_41 | Capability.SECURE_CONNECTION
CHARSET = Charset.UTF8_GENERAL_CI
STATUS = Status.AUTOCOMMIT


class Server(StreamRequestHandler):
  db = None
  packet_number = -1
  connected = False

  capabilities = 0
  max_packet = 0
  charset = 0
  attributes = {}
  username = ""
  schema = ""

  def send_packet(self, payload):
    if type(payload) is BytesIO:
      payload = payload.getvalue()

    if self.packet_number < 255:
      self.packet_number += 1
    else:
      self.packet_number = 0
    info("SENDING [%d] (%d)...", self.packet_number, len(payload))

    header = len(payload) | self.packet_number << 24
    self.wfile.write(pack_long(header))
    self.wfile.write(payload)

    debug(payload)

  def send_handshake(self):
    info("HELLO...")
    thread = int(current_thread().name.split("-")[-1])

    payload = BytesIO()
    payload.write(pack_byte(10)) # protocol version
    payload.write(pack_nullstring(self.db.version)) # server version
    payload.write(pack_long(thread)) # connection id
    payload.write(pack_padding(8)) # auth-plugin-data-part-1
    payload.write(pack_padding()) # filler
    payload.write(pack_integer(CAPABILITIES)) # capability flags

    payload.write(pack_byte(CHARSET)) # character set
    payload.write(pack_integer(STATUS)) # status flags
    payload.write(pack_integer(CAPABILITIES >> 16)) # capability flags

    payload.write(pack_padding())

    payload.write(pack_padding(10)) # reserved

    # auth-plugin-data-part-2
    if CAPABILITIES & Capability.SECURE_CONNECTION:
      payload.write(pack_padding(13))

    self.send_packet(payload)

  def send_ok(self, affected_rows=0, last_insert_id=0, warnings=0):
    payload = BytesIO()

    payload.write(pack_padding()) # header
    payload.write(pack_varinteger(affected_rows)) # affected_rows
    payload.write(pack_varinteger(last_insert_id)) # last_insert_id

    if self.capabilities & Capability.PROTOCOL_41:
      payload.write(pack_integer(STATUS)) # status_flags
      payload.write(pack_integer(warnings)) # warnings

    self.send_packet(payload)

  def send_eof(self, warnings=0):
    payload = BytesIO()
    payload.write(pack_byte(0xfe)) # header
    payload.write(pack_integer(warnings)) # warnings
    payload.write(pack_integer(STATUS))

    self.send_packet(payload)

  def send_error(self, message="", error=1064, state="42000"):
    payload = BytesIO()
    payload.write(pack_byte(0xff)) # header
    payload.write(pack_integer(error)) # error_code

    if self.capabilities & Capability.PROTOCOL_41:
      payload.write(pack_fixedstring("#")) # sql_state_marker
      payload.write(pack_fixedstring(state)) # sql_state

    payload.write(pack_fixedstring(message)) # error_message

    self.send_packet(payload)

  def send_unsupported(self, error=""):
    message = "This version of SQLite doesn't yet support '%s'" % error
    self.send_error(message, 1235)

  def send_columndef(self, name, field, length, decimals):
    payload = BytesIO()

    collation = Charset.UTF8_GENERAL_CI if field == FieldType.VAR_STRING else Charset.BINARY

    if field in [FieldType.DECIMAL, FieldType.DOUBLE]: # total length
      length += decimals
    elif field == FieldType.VAR_STRING: # utf8 = char * 3
      length *= 3

    payload.write(pack_string("def")) # catalog
    payload.write(pack_padding()) # schema
    payload.write(pack_padding()) # table

    payload.write(pack_padding()) # org_table

    payload.write(pack_string(name)) # name
    payload.write(pack_string()) # org_name

    payload.write(pack_byte(0x0c)) # length of fixed-length fields
    payload.write(pack_integer(collation)) # character set
    payload.write(pack_long(length)) # column length
    payload.write(pack_byte(field)) # type
    payload.write(pack_padding(2)) # flags
    payload.write(pack_byte(decimals)) # decimals
    payload.write(pack_padding(2)) # filler

    self.send_packet(payload)

  def send_resultset(self, data):
    meta, rows = data
    self.send_packet(pack_byte(len(meta)))

    for name, field in meta:
      field, length, decimals = self.db.internal_type(field)

      self.send_columndef(name, field, length, decimals)

    self.send_eof()

    for row in rows:
      payload = BytesIO()

      for value in row:
        payload.write(pack_resstring(value))

      self.send_packet(payload)

    self.send_eof()

  def handle_handshake(self, payload):
    self.capabilities, self.max_packet, self.charset = \
      read_data(payload, "<IIb23x")
    self.username = read_string(payload)

    if self.capabilities & Capability.SECURE_CONNECTION:
      read_varstring(payload)
    else:
      read_string(payload)

    database = ""

    if self.capabilities & Capability.CONNECT_WITH_DB:
      database = read_string(payload)
      self.use_database(database)
    else:
      self.send_ok()

    self.connected = True

  def use_database(self, name):
    if name in self.db.get_databases():
      self.send_ok()
      self.schema = name
    else:
      message = "Access denied for user '%s'@'%s' to database '%s'" % \
        (self.username, self.client_address[0], name)
      self.send_error(message, 1044)

  def _extract_last(self, query):
    return query.split(" ")[-1].split(".")[-1].strip("`").strip("[]")

  def handle(self):
    self.db = Database(self.server.path)
    self.send_handshake()

    while True:
      header = read_data(self.rfile, "<I")[0]
      self.packet_number = header >> 24
      size = self.packet_number << 24 ^ header

      info("RECEIVING [%d] (%d)...", self.packet_number, size)
      payload = BytesIO()
      payload.write(self.rfile.read(size))
      payload.seek(0)

      debug(payload.getvalue())

      if not self.connected:
        self.handle_handshake(payload)
      else:
        command = read_data(payload, "<B")[0]

        if command == Command.QUERY:
          query = read_string(payload).strip().strip(";")
          info("QUERY: %s", query)
          keywords = query.upper().split(" ", 3)

          if keywords[:2] == ["SHOW", "DATABASES"]:
            self.send_resultset(self.db.show_databases())
          elif keywords[:2] == ["SHOW", "TABLES"]:
            self.send_resultset(self.db.show_tables())
          elif keywords[:2] == ["SHOW", "COLUMNS"]:
            table = self._extract_last(query)
            self.send_resultset(self.db.show_columns(table))
          elif keywords[:3] == ["SHOW", "FULL", "COLUMNS"]:
            table = self._extract_last(query)
            self.send_resultset(self.db.show_columns(table, True))
          elif keywords[:3] == ["SHOW", "CREATE", "TABLE"]:
            name = self._extract_last(query)
            self.send_resultset(self.db.show_create_table(name))
          elif keywords[:2] == ["SHOW", "INDEX"]:
            table = self._extract_last(query)
            self.send_resultset(self.db.show_indexes(table))
          elif keywords[:2] == ["SHOW", "VARIABLES"] or \
              keywords[:2] == ["SHOW", "STATUS"]:
            self.send_resultset(self.db.show_variables())
          elif keywords[:2] == ["SHOW", "ENGINES"]:
            self.send_resultset(self.db.show_engines())
          elif keywords[:2] == ["SHOW", "COLLATION"]:
            self.send_resultset(self.db.show_collation())
          elif keywords[:3] == ["SHOW", "CHARACTER", "SET"]:
            self.send_resultset(self.db.show_charset())
          elif keywords[0] == "SHOW" or keywords[0] == "SET":
            self.send_ok()
          elif keywords[0] == "USE":
            name = self._extract_last(query)
            info("USE DATABASE %s", name)
            self.use_database(name)
          elif keywords[0] in ["CREATE", "ALTER", "DROP", "RENAME",
                               "TRUNCATE", "LOAD", "INSERT", "UPDATE",
                               "REPLACE", "DELETE", "GRANT", "REVOKE",
                               "ANALYZE", "OPTIMIZE", "REPAIR"]:
            message = "Access denied for user '%s'@'%s' to database '%s'" % \
              (self.username, self.client_address[0], self.schema)
            self.send_error(message, 1044)
          elif keywords[0] == "HELP":
            message = "Help database is corrupt or does not exist"
            self.send_error(message, 1244, "HY000")
          else:
            try:
              self.send_resultset(self.db.execute(query))
            except Exception as e:
              print_exc()
              self.send_error(str(e))
        elif command == Command.INIT_DB:
          name = read_string(payload)
          info("USE DATABASE %s", name)
          self.use_database(name)
        elif command == Command.QUIT:
          info("BYE...")
          break
        elif command == Command.PING:
          info("PING...")
          self.send_ok()
        else:
          if command in Command.__members__.values():
            self.send_unsupported(Command(command).name)
          else:
            self.send_unsupported("UNKNOWN")
