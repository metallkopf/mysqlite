#!/usr/bin/env python

from sqlite3 import connect, Row
from os.path import isfile, basename, splitext
from definitions import *


class Database:
  db = None
  name = ""

  def __init__(self, path):
    if isfile(path):
      self.db = connect("file:%s?mode=ro" % path, uri=True)
      self.db.row_factory = self._dict_factory
      self.name = splitext(basename(path))[0]
    else:
      raise FileNotFoundError()

  def _dict_factory(self, cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
      d[col[0]] = row[idx]
    return d

  def _cursor(self):
    return self.db.cursor()

  def _exec(self, query):
    return self.db.cursor().execute(query)

  def show_databases(self):
    meta = {"SCHEMA_NAME": {"table": "SCHEMATA"}}
    return meta, [{"SCHEMA_NAME": self.name}]

  def show_tables(self):
    meta = {"TABLE_NAME": {"table": "TABLE"}}
    sql = "SELECT name AS TABLE_NAME FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    result = self._exec(sql)
    data = result.fetchall()

    return meta, data

  def _convert_type(self, name):
    name = name.upper()
    if "INT" in name:
      return "int"
    else:
      return "text"

  def show_columns(self, name, full=False):
    if False:
      raise Exception

    sql = "PRAGMA table_info(%s)" % name
    result = self._exec(sql)

    columns = ["Field", "Type", "Null", "Key", "Default", "Extra"]
    meta = data = []

    for column in columns:
      item = {"name": column, "table": "COLUMNS"}
      data.append(item)

    rows = result.fetchall()

    for row in rows:
      name = row["name"]
      key = "PRI" if row["pk"] == 1 else ""
      null = "YES" if row["notnull"] == 0 else "NO"
      extra = "auto_increment" if key == "PRI" and null == "NO" and row["type"] == "INTEGER" else ""
      default = row["dflt_value"]
      field = self._convert_type(row["type"])

      data.append([name, field, null, key, default, extra])

    return meta, data
