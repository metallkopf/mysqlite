#!/usr/bin/env python

from apsw import Connection, SQLITE_ACCESS_READ
from os.path import isfile, basename, splitext
from definitions import *


class Database:
  db = None
  name = ""

  def __init__(self, path):
    if isfile(path):
      self.db = Connection(path, SQLITE_ACCESS_READ)
      self.name = splitext(basename(path))[0]
    else:
      raise FileNotFoundError()

  def _exec(self, query):
    return self.db.cursor().execute(query)

  def show_databases(self):
    meta = (("SCHEMA_NAME", "TEXT"), )
    data = [(self.name, )]
    return meta, data

  def show_tables(self):
    sql = "SELECT name AS TABLE_NAME FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    return self.exec_query(sql)

  def visible_type(self, name):
    name = name.upper()
    if "INT" in name:
      return "int"
    else:
      return "text"

  def internal_type(self, name):
    name = name.upper()
    if "INT" in name:
      return FieldType.LONGLONG
    else:
      return FieldType.VAR_STRING

  def show_columns(self, name, full=False):
    if False:
      raise Exception

    sql = "PRAGMA table_info(%s)" % name
    result = self._exec(sql)

    meta = (("Field", "TEXT"), ("Type", "TEXT"), ("Null", "TEXT"),
            ("Key", "TEXT"), ("Default", "TEXT"), ("Extra", "TEXT"))
    data = []

    for row in result:
      name = row[1]
      key = "PRI" if row[5] == 1 else ""
      null = "YES" if row[3] == 0 else "NO"
      extra = "auto_increment" if key == "PRI" and null == "NO" and row[2] == "INTEGER" else ""
      default = row[4]
      field = self.visible_type(row[2])

      data.append([name, field, null, key, default, extra])

    return meta, data

  def exec_query(self, sql):
    result = self._exec(sql)

    return result.getdescription(), result.fetchall()
