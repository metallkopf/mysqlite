#!/usr/bin/env python

from socketserver import StreamRequestHandler
from threading import current_thread
from binascii import hexlify
from io import BytesIO
from database import Database
from sqlparse import parse
from definitions import *
from utils import *
from logging import debug, info, warning, error


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

    self.wfile.write(pack_integer(len(payload)))
    self.wfile.write(pack_padding())
    self.wfile.write(pack_byte(self.packet_number))
    self.wfile.write(payload)

    debug(hexlify(payload).decode())
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

    if CAPABILITIES & Capability.PROTOCOL_41:
      payload.write(pack_integer(STATUS)) # status_flags
      payload.write(pack_integer(warnings)) # warnings

    self.send_packet(payload)

  def send_eof(self, warnings=0):
    payload = BytesIO()
    payload.write(pack_byte(0xfe)) # header
    payload.write(pack_integer(warnings)) # warnings
    payload.write(pack_integer(STATUS))

    self.send_packet(payload)

  def send_error(self, message="", error=1064):
    payload = BytesIO()
    payload.write(pack_byte(0xff)) # header
    payload.write(pack_integer(error)) # error_code

    if self.capabilities & Capability.PROTOCOL_41:
      payload.write(pack_fixedstring("#")) # sql_state_marker
      payload.write(pack_fixedstring("42000")) # sql_state

    payload.write(pack_fixedstring(message)) # error_message

    self.send_packet(payload)

  def send_columndef(self, name, length, field):
    payload = BytesIO()

    decimals = 0 if field == FieldType.LONGLONG else 0x1f
    collation = Charset.BINARY if field == FieldType.LONGLONG else Charset.UTF8_GENERAL_CI

    payload.write(pack_string("def")) # catalog
    payload.write(pack_string("")) # schema
    payload.write(pack_string("")) # table

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
      field = self.db.internal_type(field)
      length = 20 if field == FieldType.VAR_STRING else 2 ** 16 - 1

      self.send_columndef(name, length, field)

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
      length = read_data(payload, "<b")[0]
      read_data(payload, "%ds" % length)[0]
    else:
      read_string(payload)

    if self.capabilities & Capability.CONNECT_WITH_DB:
      self.schema = read_string(payload)

    if True:
      self.send_ok()
      self.connected = True
    else:
      pass


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

      debug(hexlify(payload.getvalue()).decode())
      debug(payload.getvalue())

      if not self.connected:
        self.handle_handshake(payload)
      else:
        command = read_data(payload, "<B")[0]

        if command == Command.QUERY:
          query = read_string(payload).strip().strip(";")
          info("QUERY: %s", query)

          if query.upper() == "SHOW DATABASES":
            self.send_resultset(self.db.show_databases())
          elif query.upper() == "SHOW TABLES":
            self.send_resultset(self.db.show_tables())
          elif query.upper().startswith("SHOW COLUMNS"):
            table = query.split(" ")[-1].split(".")[-1].strip("`")
            self.send_resultset(self.db.show_columns(table))
          elif query.upper().startswith("SHOW FULL COLUMNS"):
            table = query.split(" ")[-1].split(".")[-1].strip("`")
            self.send_resultset(self.db.show_columns(table, True))
          elif query.upper().startswith("SHOW CREATE TABLE"):
            name = query.split(" ")[-1].split(".")[-1].strip("`")
            self.send_resultset(self.db.show_create_table(name))
          elif query.upper().startswith("SHOW INDEX"):
            table = query.split(" ")[-1].split(".")[-1].strip("`")
            self.send_resultset(self.db.show_indexes(table))
          elif query.upper().startswith("SHOW VARIABLES") or \
            query.upper().startswith("SHOW STATUS"):
            self.send_resultset(self.db.show_variables())
          elif query.upper().startswith("SHOW ENGINES"):
            self.send_resultset(self.db.show_engines())
          elif query.upper().startswith("SHOW COLLATION"):
            self.send_resultset(self.db.show_collation())
          elif query.upper().startswith("SHOW CHARACTER SET"):
            self.send_resultset(self.db.show_charset())
          elif query.upper().startswith("SHOW"):
            self.send_ok()
          elif query.upper().startswith("SET"):
            self.send_ok()
          elif query.upper().startswith("USE"):
            self.schema = query.split(" ")[-1].strip("`")
            info("USE DATABASE %s", self.schema)
            self.send_ok()
          elif query.upper().startswith("CREATE") or \
              query.upper().startswith("ALTER") or \
              query.upper().startswith("DROP") or \
              query.upper().startswith("RENAME") or \
              query.upper().startswith("TRUNCATE") or \
              query.upper().startswith("LOAD") or \
              query.upper().startswith("INSERT") or \
              query.upper().startswith("UPDATE") or \
              query.upper().startswith("REPLACE") or \
              query.upper().startswith("DELETE") or \
              query.upper().startswith("GRANT") or \
              query.upper().startswith("REVOKE") or \
              query.upper().startswith("ANALYZE") or \
              query.upper().startswith("OPTIMIZE") or \
              query.upper().startswith("REPAIR"):
            message = "Access denied for user '%s'@'%s' to database '%s'" % \
              self.username, self.client_address[0], self.db.name
            self.send_error(message, 1044)
          else:
            try:
              self.send_resultset(self.db.execute(query))
            except Exception as e:
              self.send_error(str(e))
        elif command == Command.INIT_DB:
          self.schema = read_string(payload)
          info("USE DATABASE %s", self.schema)
          self.send_ok()
        elif command == Command.QUIT:
          info("BYE...")
          break
        elif command == Command.PING:
          info("PING...")
          self.send_ok()
        else:
          self.send_error()
