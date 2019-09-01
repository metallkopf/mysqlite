#!/usr/bin/env python

from apsw import Connection, SQLITE_ACCESS_READ, sqlitelibversion, ExecutionCompleteError
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
    query = "SELECT name AS [Table], sql AS [Create Table] FROM sqlite_master WHERE type = 'table' AND name = ?"
    return self.execute(query, (name, ))

  def show_variables(self):
    return (("Variable_name", "TEXT"), ("Value", "TEXT")), []

  def _index_details(self, name):
    query = "PRAGMA index_xinfo([%s])" % name
    return self._execute(query).fetchone()

  def _column_details(self, table, name):
    query = "PRAGMA table_info([%s])" % table

    for row in self._execute(query):
      if row[1] == name:
        return row

    return None

  def _calc_cardinality(self, table, column):
    query = "SELECT COUNT(DISTINCT([%s])) FROM [%s]" % (column, table)
    return self._execute(query).fetchone()[0]

  def show_indexes(self, table):
    meta = (("Table", "TEXT"), ("Non_unique", "INTEGER"),
            ("Key_name", "TEXT"), ("Seq_in_index", "INTEGER"),
            ("Column_name", "TEXT"), ("Collation", "TEXT"),
            ("Cardinality", "INTEGER"), ("Sub_part", "INTEGER"),
            ("Packed", "TEXT"), ("Null", "TEXT"), ("Index_type", "TEXT"),
            ("Comment", "TEXT"), ("Index_comment", "TEXT"))
    data = []

    query = "PRAGMA index_list([%s])" % table

    for row in self._execute(query):
      if row[3] != "c":
        continue

      index = self._index_details(row[1])
      column = self._column_details(table, index[2])

      unique = int(row[2])
      key = "PRIMARY" if column[5] == 1 else row[1]
      name = index[2]
      collation = "A" if index[3] == 0 else None
      cardinality = self._calc_cardinality(table, index[2])
      null = "YES" if column[3] == 0 else None

      item = (table, unique, key, 1, name, collation, cardinality,
              None, None, null, "BTREE", "", "")
      data.append(item)

    return meta, data

  def show_charset(self):
    meta = (("Charset", "TEXT"), ("Description", "TEXT"),
            ("Default collation", "TEXT"), ("Maxlen", "INTEGER"))
    data = [("utf8", "UTF-8 Unicode", "utf8_general_ci", 3)]
    return meta, data

  def show_collation(self):
    meta = (("Collation", "TEXT"), ("Charset", "TEXT"),
            ("Id", "INTEGER"), ("Default", "TEXT"),
            ("Compiled", "TEXT"), ("Sortlen", "INTEGER"))
    data = [("utf8_general_ci", "utf8", Charset.UTF8_GENERAL_CI, "Yes", "Yes", 1)]
    return meta, data

  def show_engines(self):
    meta = (("Engine", "TEXT"), ("Support", "TEXT"), ("Comment", "TEXT"))
    data = [("SQLite", "DEFAULT", "Small. Fast. Reliable. Choose any three.")]
    return meta, data

  def visible_type(self, name):
    name = name.upper()
    if "INT" in name:
      return "int"
    else:
      return "text"

  def internal_type(self, name):
    if name is None:
      return FieldType.VAR_STRING

    name = name.upper()
    if "INT" in name:
      return FieldType.LONGLONG
    else:
      return FieldType.VAR_STRING

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

    query = "PRAGMA index_list([%s])" % name
    indexes = {}

    for row in self._execute(query):
      if row[3] == "c":
        continue

      index = self._index_details(row[1])
      indexes[index[3]] = bool(row[2])

    query = "PRAGMA table_info([%s])" % name

    for row in self._execute(query):
      name = row[1]
      key = "PRI" if row[5] == 1 else ""
      null = "YES" if row[3] == 0 else "NO"
      extra = "auto_increment" if key == "PRI" and null == "NO" and row[2] == "INTEGER" else ""
      default = row[4]
      field = self.visible_type(row[2])
      collation = "utf8_general_ci" if field != "text" else ""

      if key == "" and name in indexes:
        key = "UNI" if indexes[name] else "MUL"

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
