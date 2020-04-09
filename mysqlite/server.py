from io import BytesIO
from logging import debug, info
from socketserver import StreamRequestHandler
from threading import current_thread
from time import monotonic
from traceback import print_exc

from mysqlite.database import Database
from mysqlite.definitions import Capability, Charset, Command, FieldFlag, \
  FieldType, Status
from mysqlite.parser import guess_statement
from mysqlite.utils import pack_byte, pack_fixedstring, pack_header, \
  pack_integer, pack_long, pack_nullstring, pack_padding, pack_resstring, \
  pack_string, pack_varinteger, read_data, read_header, read_string, \
  read_varstring


CAPABILITIES = Capability.LONG_PASSWORD | Capability.FOUND_ROWS | \
  Capability.LONG_FLAG | Capability.CONNECT_WITH_DB | Capability.NO_SCHEMA | \
  Capability.PROTOCOL_41 | Capability.SECURE_CONNECTION
CHARSET = Charset.UTF8_GENERAL_CI
STATUS = Status.AUTOCOMMIT


connections = {}


class Server(StreamRequestHandler):
  thread = 0
  db = None
  number = -1
  connected = False
  packet = None
  port = 0

  capabilities = 0
  max_packet = 0
  charset = 0
  attributes = {}
  username = ""
  schema = ""

  def queue_packet(self, payload, send=False):
    if type(payload) is BytesIO:
      payload = payload.getvalue()

    if self.number < 255:
      self.number += 1
    else:
      self.number = 0

    length = len(payload)
    header = pack_header(length, self.number)
    debug(f"> {payload}")

    self.packet.write(header)
    self.packet.write(payload)

    if send:
      self.send_packets()

  def send_packets(self):
    self.wfile.write(self.packet.getvalue())
    self.packet.seek(0)
    self.packet.truncate(0)

  def send_handshake(self):
    payload = BytesIO()
    payload.write(pack_byte(10))  # protocol version
    payload.write(pack_nullstring(self.db.version))  # server version
    payload.write(pack_long(self.thread))  # connection id
    payload.write(pack_fixedstring(" " * 8))  # auth-plugin-data-part-1
    payload.write(pack_padding())  # filler
    payload.write(pack_integer(CAPABILITIES))  # capability flags

    payload.write(pack_byte(CHARSET))  # character set
    payload.write(pack_integer(STATUS))  # status flags
    payload.write(pack_integer(CAPABILITIES >> 16))  # capability flags

    payload.write(pack_padding())

    payload.write(pack_padding(10))  # reserved

    # auth-plugin-data-part-2
    if CAPABILITIES & Capability.SECURE_CONNECTION:
      payload.write(pack_fixedstring(" " * 12))
      payload.write(pack_padding())

    self.queue_packet(payload, True)

  def send_ok(self, affected_rows=0, last_insert_id=0, warnings=0):
    payload = BytesIO()

    payload.write(pack_padding())  # header
    payload.write(pack_varinteger(affected_rows))  # affected_rows
    payload.write(pack_varinteger(last_insert_id))  # last_insert_id

    if self.capabilities & Capability.PROTOCOL_41:
      payload.write(pack_integer(STATUS))  # status_flags
      payload.write(pack_integer(warnings))  # warnings

    self.queue_packet(payload, True)

  def send_eof(self, warnings=0):
    payload = BytesIO()
    payload.write(pack_byte(0xfe))  # header
    payload.write(pack_integer(warnings))  # warnings
    payload.write(pack_integer(STATUS))

    self.queue_packet(payload)

  def send_error(self, message="", code=1064, state="42000"):
    payload = BytesIO()
    payload.write(pack_byte(0xff))  # header
    payload.write(pack_integer(code))  # error_code

    if self.capabilities & Capability.PROTOCOL_41:
      payload.write(pack_fixedstring("#"))  # sql_state_marker
      payload.write(pack_fixedstring(state))  # sql_state

    payload.write(pack_fixedstring(message))  # error_message

    self.queue_packet(payload, True)

  def send_unsupported(self, command):
    self.send_error(f"This version of SQLite doesn't yet support '{command}'", 1235)

  def send_columndef(self, name, _type, length, decimals):
    payload = BytesIO()

    collation = Charset.UTF8_GENERAL_CI if _type == FieldType.VAR_STRING else Charset.BINARY
    flags = 0

    if _type in [FieldType.DECIMAL, FieldType.DOUBLE]:  # total length
      length += decimals
    elif _type == FieldType.VAR_STRING:  # utf8 = char * 3
      length *= 3
    elif _type == FieldType.TIMESTAMP:
      flags = FieldFlag.TIMESTAMP
    elif _type == FieldType.BLOB:
      flags = FieldFlag.BLOB | FieldFlag.BINARY

    payload.write(pack_string("def"))  # catalog
    payload.write(pack_padding())  # schema
    payload.write(pack_padding())  # table

    payload.write(pack_padding())  # org_table

    payload.write(pack_string(name))  # name
    payload.write(pack_padding())  # org_name

    payload.write(pack_byte(0x0c))  # length of fixed-length fields
    payload.write(pack_integer(collation))  # character set
    payload.write(pack_long(length))  # column length
    payload.write(pack_byte(_type))  # type
    payload.write(pack_integer(flags))  # flags
    payload.write(pack_byte(decimals))  # decimals
    payload.write(pack_padding(2))  # filler

    self.queue_packet(payload)

  def send_resultset(self, data):
    meta, rows = data
    self.queue_packet(pack_byte(len(meta)))

    for (name, _, _type, length, decimals) in meta:
      self.send_columndef(name, _type, length, decimals)

    self.send_eof()

    for row in rows:
      payload = BytesIO()

      for value in row:
        payload.write(pack_resstring(value))

      self.queue_packet(payload)

    self.send_eof()
    self.send_packets()

  def send_processlist(self, full):
    meta = (("Id", "INTEGER"), ("User", "VARCHAR(16)"),
            ("Host", "VARCHAR(64)"), ("db", "VARCHAR(64)"),
            ("Command", "VARCHAR(16)"), ("Time", "INTEGER"),
            ("State", "VARCHAR(30)"), ("Info", "TEXT"))
    data = []

    for connection in connections.values():
      if full is None and self.username != connection["username"]:
        continue

      command = None
      if connection["command"] in Command.__members__.values():
        command = Command(connection["command"]).name.capitalize()

      item = (connection["thread"], connection["username"],
              connection["host"], connection["schema"], command,
              int(monotonic() - connection["time"]), "", None)
      data.append(item)

    self.send_resultset((self.db.expand_meta(meta), data))

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
      connections[self.port]["schema"] = self.schema
    else:
      message = f"Access denied for user '{self.username}'@'{self.client_address[0]}' to database '{name}'"
      self.send_error(message, 1044)

  def _extract_table(self, text):
    return text.replace("`", "").split(".")[-1]

  def process_query(self, text):
    function, params = guess_statement(text)

    if function is None:
      return False

    if function == "show_databases":
      self.send_resultset(self.db.show_databases())
    elif function == "show_tables":
      self.send_resultset(self.db.show_tables())
    elif function == "show_columns":
      table = self._extract_table(params["table"])
      self.send_resultset(self.db.show_columns(table, params["modifier"]))
    elif function == "show_create_table":
      name = self._extract_table(params["name"])
      self.send_resultset(self.db.show_create_table(name))
    elif function == "show_index":
      table = self._extract_table(params["table"])
      self.send_resultset(self.db.show_indexes(table))
    elif function == "show_variables" or function == "show_status":
      self.send_resultset(self.db.show_variables())
    elif function == "show_engines":
      self.send_resultset(self.db.show_engines())
    elif function == "show_collation":
      self.send_resultset(self.db.show_collation())
    elif function == "show_character_set":
      self.send_resultset(self.db.show_charset())
    elif function == "show_table_status":
      self.send_resultset(self.db.show_table_status(params["pattern"]))
    elif function == "use":
      self.use_database(params["database"])
    elif function == "show_processlist":
      self.send_processlist(params["modifier"])
    elif function == "help":
      self.send_error("Help database is corrupt or does not exist", 1244, "HY000")
    else:
      return False

    return True

  def handle(self):
    self.db = Database(self.server.filename)
    self.thread = int(current_thread().name.split("-")[-1])
    self.packet = BytesIO()
    self.send_handshake()
    self.port = self.client_address[1]
    connections[self.port] = {"thread": self.thread, "username": None,
                              "host": f"{self.client_address[0]}:{self.client_address[1]}",
                              "schema": None, "time": monotonic(),
                              "command": Command.CONNECT.value}

    while True:
      length, self.number = read_header(self.rfile)

      payload = BytesIO()
      payload.write(self.rfile.read(length))
      payload.seek(0)

      debug(f"< {payload.getvalue()}")

      if not self.connected:
        self.handle_handshake(payload)
        connections[self.port]["username"] = self.username
        connections[self.port]["time"] = monotonic()
        connections[self.port]["command"] = Command.SLEEP.value
        continue

      command = read_data(payload, "<B")[0]
      connections[self.port]["command"] = command
      connections[self.port]["time"] = monotonic()

      if command == Command.QUERY:
        query = read_string(payload).strip().strip(";")
        info(f"QUERY: {query}")

        keyword = query.split(" ", 1)[0].upper()

        if keyword == "SELECT":
          try:
            self.send_resultset(self.db.execute(query))
          except Exception as e:
            print_exc()
            self.send_error(str(e))
        elif not self.process_query(query):
          if keyword == "SET":
            self.send_ok()
          else:
            message = f"Access denied for user '{self.username}'@'{self.client_address[0]}' to database '{self.schema}'"
            self.send_error(message, 1044)
      elif command == Command.INIT_DB:
        name = read_string(payload)
        self.use_database(name)
      elif command == Command.QUIT:
        break
      elif command == Command.PING:
        self.send_ok()
      else:
        if command in Command.__members__.values():
          self.send_unsupported(Command(command).name)
        else:
          self.send_unsupported("UNKNOWN")

      connections[self.port]["time"] = monotonic()
      connections[self.port]["command"] = Command.SLEEP.value

  def finish(self):
    super().finish()
    del connections[self.port]
