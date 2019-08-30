#!/usr/bin/env python

from socketserver import StreamRequestHandler
from threading import current_thread
from binascii import hexlify
from io import BytesIO
from definitions import *
from utils import *


PROTOCOL_VERSION = 10
SERVER_VERSION = "4.1.25" # last 4.1 release
SERVER_CAPABILITIES = Capability.LONG_PASSWORD | Capability.FOUND_ROWS | \
  Capability.LONG_FLAG | Capability.NO_SCHEMA | Capability.PROTOCOL_41 | \
    Capability.SECURE_CONNECTION
SERVER_CHARSET = Charset.UTF8_GENERAL_CI
SERVER_STATUS = Status.AUTOCOMMIT
SERVER_CAPABILITIES2 = 0


class Server(StreamRequestHandler):
  packet_number = -1
  thread = 0
  connected = False

  client_capabilities = 0
  max_packet = 0
  charset = 0
  username = ""
  database = ""

  def send_packet(self, payload):
    if type(payload) is BytesIO:
      payload = payload.getvalue()

    self.packet_number += 1
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
    payload.write(pack_nullstring(SERVER_VERSION)) # server version
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
   payload.write(pack_byte(0xfe))
   payload.write(pack_integer(warnings))
   payload.write(pack_integer(SERVER_STATUS))

   self.send_packet(payload)

  def send_columndef(self, name, length, field, table=""):
    payload = BytesIO()

    decimals = 0 if field == FieldType.LONGLONG else 0x1f
    collation = Charset.BINARY if field == FieldType.LONGLONG else Charset.UTF8_GENERAL_CI

    payload.write(pack_string("def")) # catalog
    payload.write(pack_string(self.database)) # schema
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

  def send_resultset(self, rows, meta):
    if rows is None or len(rows) == 0:
      self.send_ok()
    else:
      columns = [aux["name"] for aux in meta]
      fields = {}

      self.send_packet(pack_byte(len(columns)))

      for definition in meta:
        name = definition["name"]
        length = definition.get("length", 255)
        field = definition.get("type", FieldType.VAR_STRING)
        table = definition.get("table", "")

        if field == FieldType.LONGLONG:
          length = 20

        fields[name] = field

        self.send_columndef(name, length, field, table)

      self.send_eof()

      for row in rows:
        payload = BytesIO()

        for column, field in fields.items():
          if field == FieldType.LONGLONG:
            payload.write(pack_varinteger(row[column]))
          else:
            payload.write(pack_string(row[column]))

        self.send_packet(payload)

      self.send_eof()
      self.wfile.flush()

  def show_databases(self):
    self.send_resultset([{"name": "test"}], [{"name": "name", "table": "databases"}])

  def show_tables(self):
    self.send_resultset([{"name": "temp"}], [{"name": "name", "table": "tables"}])

  def show_columns(self):
    data = [{"Field": "id", "Type": "INTEGER", "Null": "NO",
             "Key": "PRI", "Default": None, "Extra": "auto_increment"},
            {"Field": "value", "Type": "TEXT", "Null": "YES",
             "Key": "", "Default": None, "Extra": ""}]
    meta = [{"name": "Field", "table": "columns"},
            {"name": "Type", "table": "columns"},
            {"name": "Null", "table": "columns"},
            {"name": "Key", "table": "columns"},
            {"name": "Default", "table": "columns"},
            {"name": "Extra", "table": "columns"}]
    self.send_resultset(data, meta)

  def handle(self):
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
          query = read_string(payload)
          print(self.thread, "QUERY:", query)

          if query.upper() == "SHOW DATABASES":
            self.show_databases()
          elif query.upper() == "SHOW TABLES":
            self.show_tables()
          elif query.upper().startswith("SHOW COLUMNS"):
            self.show_columns()
          elif query.upper().startswith("SET "):
            self.send_ok()
          else:
            self.send_ok()
        elif command == Command.INIT_DB:
          self.database = read_string(payload)
          print("USE DATABASE", self.database)
          self.send_ok()
        elif command == Command.QUIT:
          self.connected = False
          print(self.thread, "BYE...")
          break
        else:
          pass
