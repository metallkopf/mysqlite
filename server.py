#!/usr/bin/env python

from socketserver import StreamRequestHandler
from threading import current_thread
from binascii import hexlify
from io import BytesIO
from database import Database
from sqlparse import parse
from definitions import *
from utils import *


PROTOCOL_VERSION = 10
SERVER_CAPABILITIES = Capability.LONG_PASSWORD | Capability.FOUND_ROWS | \
  Capability.LONG_FLAG | Capability.NO_SCHEMA | Capability.PROTOCOL_41 | \
    Capability.SECURE_CONNECTION
SERVER_CHARSET = Charset.UTF8_GENERAL_CI
SERVER_STATUS = Status.AUTOCOMMIT
SERVER_CAPABILITIES2 = 0


class Server(StreamRequestHandler):
  db = None
  packet_number = -1
  thread = 0
  connected = False

  client_capabilities = 0
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
    print(self.thread, "SENDING [%d] (%d)..." % (self.packet_number, len(payload)))

    header = BytesIO()
    header.write(pack_integer(len(payload)))
    header.write(pack_padding())
    header.write(pack_byte(self.packet_number))

    self.wfile.write(header.getvalue())
    self.wfile.write(payload)
    print("", hexlify(payload).decode())
    print("", payload)

  def send_handshake(self):
    print(self.thread, "HELLO...")

    payload = BytesIO()
    payload.write(pack_byte(PROTOCOL_VERSION)) # protocol version
    payload.write(pack_nullstring(self.db.version)) # server version
    payload.write(pack_long(self.thread)) # connection id
    payload.write(pack_padding(8)) # auth-plugin-data-part-1
    payload.write(pack_padding()) # filler
    payload.write(pack_integer(SERVER_CAPABILITIES)) # capability flags

    payload.write(pack_byte(SERVER_CHARSET)) # character set
    payload.write(pack_integer(SERVER_STATUS)) # status flags
    payload.write(pack_integer(SERVER_CAPABILITIES2 >> 16)) # capability flags

    payload.write(pack_padding())

    payload.write(pack_padding(10)) # reserved

    # auth-plugin-data-part-2
    if SERVER_CAPABILITIES & Capability.SECURE_CONNECTION:
      payload.write(pack_padding(13))

    self.send_packet(payload)

  def send_ok(self, affected_rows=0, last_insert_id=0, warnings=0):
    payload = BytesIO()

    payload.write(pack_padding()) # header
    payload.write(pack_varinteger(affected_rows)) # affected_rows
    payload.write(pack_varinteger(last_insert_id)) # last_insert_id

    if SERVER_CAPABILITIES & Capability.PROTOCOL_41:
      payload.write(pack_integer(SERVER_STATUS)) # status_flags
      payload.write(pack_integer(warnings)) # warnings

    self.send_packet(payload)

  def send_eof(self, warnings=0):
    payload = BytesIO()
    payload.write(pack_byte(0xfe)) # header
    payload.write(pack_integer(warnings)) # warnings
    payload.write(pack_integer(SERVER_STATUS))

    self.send_packet(payload)

  def send_error(self, message="", error=1064):
    payload = BytesIO()
    payload.write(pack_byte(0xff)) # header
    payload.write(pack_integer(error)) # error_code

    if SERVER_CAPABILITIES & Capability.PROTOCOL_41:
      payload.write(pack_fixedstring("#")) # sql_state_marker
      payload.write(pack_fixedstring("42000")) # sql_state

    payload.write(pack_fixedstring(message)) # error_message

    self.send_packet(payload)

  def send_columndef(self, name, length, field, table=""):
    payload = BytesIO()

    decimals = 0 if field == FieldType.LONGLONG else 0x1f
    collation = Charset.BINARY if field == FieldType.LONGLONG else Charset.UTF8_GENERAL_CI

    payload.write(pack_string("def")) # catalog
    payload.write(pack_string(self.schema)) # schema
    payload.write(pack_string(table)) # table

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

  def handle(self):
    self.db = Database(self.server.path)
    self.thread = int(current_thread().name.split("-")[-1])
    self.send_handshake()

    while True:
      header = read_data(self.rfile, "<I")[0]
      self.packet_number = header >> 24
      size = self.packet_number << 24 ^ header

      print(self.thread, "RECEIVING [%d] (%d)..." % (self.packet_number, size))
      payload = BytesIO()
      payload.write(self.rfile.read(size))
      payload.seek(0)

      print("", hexlify(payload.getvalue()).decode())
      print("", payload.getvalue())

      if not self.connected:
        self.client_capabilities, self.max_packet, self.charset = \
          read_data(payload, "<IIb23x")
        self.username = read_string(payload)
        #length = read_data(payload, "<b")[0]
        #auth_response = read_data(payload, "%ds" % length)[0]

        if True:
          self.send_ok()
          self.connected = True
        else:
          pass
      else:
        command = read_data(payload, "<B")[0]

        if command == Command.QUERY:
          query = read_string(payload).strip().strip(";")
          print(self.thread, "QUERY:", query)

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
            print("USE DATABASE", self.schema)
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
          print("USE DATABASE", self.schema)
          self.send_ok()
        elif command == Command.QUIT:
          print(self.thread, "BYE...")
          break
        elif command == Command.PING:
          print(self.thread, "PING...")
          self.send_ok()
        else:
          self.send_error()
