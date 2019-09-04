#!/usr/bin/env python

from apsw import Connection, SQLITE_ACCESS_READ, sqlitelibversion, ExecutionCompleteError
from re import match
from definitions import *


class Database:
  inst = None
  version = ""

  def __init__(self, path):
    self.inst = Connection(path, SQLITE_ACCESS_READ)
    self.version = sqlitelibversion() + "-SQLite"

  def _execute(self, query, params=None):
    cursor = self.inst.cursor()
    cursor.setexectrace(self._exectrace) # save getdescription columns

    return cursor.execute(query, params)

  def get_databases(self):
    return ["main"]

  def get_tables(self):
    tables = []
    query = "SELECT name AS TABLE_NAME FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"

    for row in self._execute(query):
      tables.append(row[0])

    return tables

  def show_databases(self):
    meta = (("SCHEMA_NAME", "TEXT"), )
    data = []

    for name in self.get_databases():
      data.append((name, ))

    return meta, data

  def show_tables(self):
    meta = (("TABLE_NAME", "TEXT"), )
    data = []

    for name in self.get_tables():
      data.append((name, ))

    return meta, data

  def show_create_table(self, name):
    meta = (("Table", "TEXT"), ("Create Table", "TEXT"))
    lines = []
    primaries = []
    extra = []

    for column in self._column_list(name):
      line = " %s" % column["name"]
      line += " %s" % column["type"]

      if not column["nullable"]:
        line += " NOT NULL"

      if column["default"] is not None:
        line += " DEFAULT '%s'" % column["default"]

      if column["serial"]:
        line += " AUTO_INCREMENT"

      lines.append(line)

      if column["primary"]:
        primaries.append(column["name"])

      if column["index"]:
        line = "%s KEY %s (%s%s)" % (" UNIQUE" if column["unique"] else "",
                                     column["index"], column["name"],
                                     " ASC" if column["order"] == 1 else "")
        extra.append(line)

      if column["foreign"]:
        line = " FOREIGN KEY (%s) REFERENCES %s(%s)" % (column["name"],
                                                        column["table"],
                                                        column["foreign"])
        extra.append(line)

    if primaries:
      lines.append(" PRIMARY KEY (%s)" % ", ".join(primaries))

    extra.sort(reverse=True)
    lines.extend(extra)

    definition = "CREATE TABLE %s (\n%s\n) ENGINE=SQLite" % (name, ",\n".join(lines))

    return meta, [[name, definition]]

  def show_variables(self):
    return (("Variable_name", "TEXT"), ("Value", "TEXT")), []

  def _index_list(self, table):
    query = "PRAGMA index_list([%s])" % table
    indexes = {}

    for row in self._execute(query):
      query = "PRAGMA index_xinfo([%s])" % row[1]
      details = self._execute(query).fetchone()

      indexes[details[2]] = {"index": row[1], "unique": bool(row[2]),
                             "order": 1 if details[3] == 0 else -1}

    return indexes

  def _foreign_list(self, table):
    query = "PRAGMA foreign_key_list([%s])" % table
    foreigns = {}

    for row in self._execute(query):
      foreigns[row[3]] = {"table": row[2], "foreign": row[4],
                          "update": row[5], "delete": row[6]}

    return foreigns

  def _column_list(self, table):
    indexes = self._index_list(table)
    foreigns = self._foreign_list(table)
    query = "PRAGMA table_info([%s])" % table
    columns = []

    for row in self._execute(query):
      column = {"name": row[1], "type": self.visible_type(row[2]),
                "nullable": row[3] == 0, "primary": bool(row[5]),
                "default": None if row[4] == "NULL" else row[4],
                "index": False, "unique": None, "order": None,
                "table": None, "foreign": None, "update": None,
                "delete": None, "serial": False}

      if "int" in column["type"] and column["primary"] and \
          not column["nullable"]:
        column["serial"] = True

      if row[1] in indexes:
        column.update(indexes[row[1]])
      if row[1] in foreigns:
        column.update(foreigns[row[1]])

      columns.append(column)

    return columns

  def _calc_cardinality(self, table, column):
    query = "SELECT COUNT(DISTINCT([%s])) FROM [%s]" % (column, table)
    return self._execute(query).fetchone()[0]

  def _count_rows(self, table):
    query = "SELECT COUNT(1) FROM [%s]" % table
    return self._execute(query).fetchone()[0]

  def _next_id(self, table):
    primary = None

    for column in self._column_list(table):
      if column["serial"]:
        primary = column["name"]
        break
    else:
      return None

    query = "SELECT COUNT([%s]) FROM [%s]" % (primary, table)
    return self._execute(query).fetchone()[0] + 1

  def show_indexes(self, table):
    meta = (("Table", "TEXT"), ("Non_unique", "INTEGER"),
            ("Key_name", "TEXT"), ("Seq_in_index", "INTEGER"),
            ("Column_name", "TEXT"), ("Collation", "TEXT"),
            ("Cardinality", "INTEGER"), ("Sub_part", "INTEGER"),
            ("Packed", "TEXT"), ("Null", "TEXT"), ("Index_type", "TEXT"),
            ("Comment", "TEXT"), ("Index_comment", "TEXT"))
    data = []

    for column in self._column_list(table):
      if not column["index"]:
        continue

      unique = int(not column["unique"])
      key = "PRIMARY" if column["primary"] else column["index"]
      collation = "A" if column["order"] == 1 else None
      cardinality = self._calc_cardinality(table, column["name"])
      null = "YES" if column["nullable"] else None

      item = (table, unique, key, 1, column["name"], collation,
              cardinality, None, None, null, "BTREE", "", "")
      data.append(item)

    return meta, data

  def show_charset(self):
    meta = (("Charset", "TEXT"), ("Description", "TEXT"),
            ("Default collation", "TEXT"), ("Maxlen", "INTEGER"))
    data = [("utf8", "UTF-8 Unicode", Charset.UTF8_GENERAL_CI.name.lower(), 3)]
    return meta, data

  def show_collation(self):
    meta = (("Collation", "TEXT"), ("Charset", "TEXT"),
            ("Id", "INTEGER"), ("Default", "TEXT"),
            ("Compiled", "TEXT"), ("Sortlen", "INTEGER"))
    data = [(Charset.UTF8_GENERAL_CI.name.lower(), "utf8",
             Charset.UTF8_GENERAL_CI, "Yes", "Yes", 1)]
    return meta, data

  def show_engines(self):
    meta = (("Engine", "TEXT"), ("Support", "TEXT"), ("Comment", "TEXT"))
    data = [("SQLite", "DEFAULT", "Small. Fast. Reliable. Choose any three.")]
    return meta, data

  def show_table_status(self, name=None):
    meta = (("Name", "TEXT"), ("Engine", "TEXT"), ("Version", "INTEGER"),
            ("Row_format", "TEXT"), ("Rows", "INTEGER"),
            ("Avg_row_length", "INTEGER"), ("Data_length", "INTEGER"),
            ("Max_data_length", "INTEGER"), ("Index_length", "INTEGER"),
            ("Data_free", "INTEGER"), ("Auto_increment", "INTEGER"),
            ("Create_time", "TEXT"), ("Update_time", "TEXT"),
            ("Check_time", "TEXT"), ("Collation", "TEXT"),
            ("Checksum", "INTEGER"), ("Create_options", "TEXT"),
            ("Comment", "TEXT"))
    data = []
    tables = [name] if name else self.get_tables()

    for table in tables:
      rows = self._count_rows(table)
      collation = Charset.UTF8_GENERAL_CI.name.lower()
      auto = self._next_id(table)
      data.append((table, "SQLite", 9, "Dynamic", rows, 0, 0, None, 0, 0,
                   auto, None, None, None, collation, None, "", ""))

    return meta, data

  def visible_type(self, name):
    field, length, decimals = self.internal_type(name)

    if field == FieldType.LONGLONG:
      return "int(%d)" % length
    elif field == FieldType.DECIMAL:
      return "decimal(%d,%d)" % (length, decimals)
    elif field == FieldType.DOUBLE:
      return "double(%d,%d)" % (length, decimals)
    elif field == FieldType.VAR_STRING:
      return "varchar(%d)" % length
    else:
      return "text"

  def internal_type(self, name):
    if name is None:
      return FieldType.BLOB, 2 ** 16 - 1, 0x1f

    name = name.upper()
    field = name
    length = 0
    decimals = 0

    results = match("(\w+)\((\d+),(\d+)\)", name)
    if results:
      field, length, decimals = results.groups()
    else:
      results = match("(\w+)\((\d+)\)", name)
      if results:
        field, length = results.groups()

    length = int(length)
    decimals = int(decimals)

    if "INT" in name:
      return FieldType.LONGLONG, 21, 0
    elif "DECIMAL" in name or "NUMERIC" in name:
     return FieldType.DECIMAL, length, decimals
    elif "FLOAT" in name or "DOUBLE" in name or "REAL" in name:
     return FieldType.DOUBLE, length, decimals
    elif "CHAR" in name and length > 0:
      return FieldType.VAR_STRING, length, 0x1f
    else:
      return FieldType.BLOB, 2 ** 16 - 1, 0x1f

  def show_columns(self, name, full=False):
    meta = None
    data = []

    if not full:
      meta = (("Field", "TEXT"), ("Type", "TEXT"), ("Null", "TEXT"),
              ("Key", "TEXT"), ("Default", "TEXT"), ("Extra", "TEXT"))
    else:
      meta = (("Field", "TEXT"), ("Type", "TEXT"), ("Collation", "TEXT"),
              ("Null", "TEXT"), ("Key", "TEXT"), ("Default", "TEXT"),
              ("Extra", "TEXT"), ("Privileges", "TEXT"), ("Comment", "TEXT"))

    columns = self._column_list(name)

    for column in columns:
      name = column["name"]
      key = "PRI" if column["primary"] else ""
      null = "YES" if column["nullable"] else "NO"
      extra = "auto_increment" if column["serial"] else ""
      default = column["default"]
      field = column["type"]
      collation = Charset.UTF8_GENERAL_CI.name.lower() if field != "text" else ""

      if key == "" and column["index"]:
        key = "UNI" if column["unique"] else "MUL"

      if not full:
        data.append([name, field, null, key, default, extra])
      else:
        data.append([name, field, collation, null, key, default, extra, "", ""])

    return meta, data

  def _exectrace(self, cursor, sql, bindings):
    self._meta = cursor.getdescription()
    return True

  def execute(self, query, params=None):
    result = self._execute(query, params)

    try:
      return result.getdescription(), result.fetchall() # FIXME un-optimized
    except ExecutionCompleteError:
      return self._meta, [] # NOTE don't fail on empty resultset
