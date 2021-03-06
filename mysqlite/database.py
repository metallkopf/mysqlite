from re import match

from apsw import SQLITE_ACCESS_READ, Connection

from mysqlite.definitions import Charset, FieldType


class Database:
  inst = None
  version = ""

  def __init__(self, filename):
    self.inst = Connection(filename, SQLITE_ACCESS_READ)
    self.version = "4.1.25-SQLite"

  def _execute(self, query, params=None):
    cursor = self.inst.cursor()
    cursor.setexectrace(self._exectrace)  # save getdescription columns

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
    meta = (("Database", "VARCHAR(64)"), )
    data = []

    for name in self.get_databases():
      data.append((name, ))

    return self.expand_meta(meta), data

  def show_tables(self):
    meta = (("Table", "VARCHAR(64)"), )
    data = []

    for name in self.get_tables():
      data.append((name, ))

    return self.expand_meta(meta), data

  def show_create_table(self, name):
    meta = (("Table", "VARCHAR(64)"), ("Create Table", "TEXT"))
    lines = []
    primaries = []
    extra = []

    for column in self._column_list(name):
      line = f"  {column['name']}"
      line += f" {column['type']}"

      if not column["nullable"]:
        line += " NOT NULL"

      if column["nullable"] or column["default"] is not None:
        if column["default"] is None:
          line += " DEFAULT NULL"
        else:
          line += f" DEFAULT '{column['default']}'"

      if column["serial"]:
        line += " AUTO_INCREMENT"

      lines.append(line)

      if column["primary"]:
        primaries.append(column["name"])

      if column["index"]:
        unique = " UNIQUE" if column["unique"] else ""
        order = " ASC" if column["order"] == 1 else ""
        line = f" {unique} KEY {column['index']} ({column['name']}{order})"
        extra.append(line)

      if column["foreign"]:
        line = "  CONSTRAINT fk_{0}_{1} FOREIGN KEY ({1}) REFERENCES {2} ({3})". \
          format(name, column["name"], column["table"], column["foreign"])
        extra.append(line)

    if primaries:
      lines.append("  PRIMARY KEY (%s)" % ", ".join(primaries))

    extra.sort(reverse=True)
    lines.extend(extra)

    definition = "CREATE TABLE %s (\n%s\n) ENGINE=SQLite" % (name, ",\n".join(lines))

    return self.expand_meta(meta), [[name, definition]]

  def show_variables(self):
    meta = (("Variable_name", "VARCHAR(30)"), ("Value", "VARCHAR(255)"))
    return self.expand_meta(meta), []

  def _index_list(self, table):
    indexes = {}

    for row in self._execute(f"PRAGMA index_list([{table}])"):
      details = self._execute(f"PRAGMA index_xinfo([{row[1]}])").fetchone()

      indexes[details[2]] = {"index": row[1], "unique": bool(row[2]),
                             "order": 1 if details[3] == 0 else -1}

    return indexes

  def _foreign_list(self, table):
    foreigns = {}

    for row in self._execute(f"PRAGMA foreign_key_list([{table}])"):
      foreigns[row[3]] = {"table": row[2], "foreign": row[4],
                          "update": row[5], "delete": row[6]}

    return foreigns

  def _column_list(self, table):
    indexes = self._index_list(table)
    foreigns = self._foreign_list(table)
    columns = []

    for row in self._execute(f"PRAGMA table_info([{table}])"):
      column = {"name": row[1], "type": self.visible_type(row[2]),
                "nullable": row[3] == 0, "primary": bool(row[5]),
                "default": None if row[4] == "NULL" else row[4],
                "index": False, "unique": None, "order": None,
                "table": None, "foreign": None, "update": None,
                "delete": None, "serial": False}

      if "int" in column["type"] and column["primary"] and not column["nullable"]:
        column["serial"] = True

      if row[1] in indexes:
        column.update(indexes[row[1]])
      if row[1] in foreigns:
        column.update(foreigns[row[1]])

      columns.append(column)

    return columns

  def _calc_cardinality(self, table, column):
    return self._execute(f"SELECT COUNT(DISTINCT([{column}])) FROM [{table}]").fetchone()[0]

  def _count_rows(self, table):
    return self._execute(f"SELECT COUNT(1) FROM [{table}]").fetchone()[0]

  def _next_id(self, table):
    primary = None

    for column in self._column_list(table):
      if column["serial"]:
        primary = column["name"]
        break
    else:
      return None

    return self._execute(f"SELECT COUNT([{primary}]) FROM [{table}]").fetchone()[0] + 1

  def show_indexes(self, table):
    meta = (("Table", "VARCHAR(64)"), ("Non_unique", "INTEGER"),
            ("Key_name", "VARCHAR(64)"), ("Seq_in_index", "INTEGER"),
            ("Column_name", "VARCHAR(64)"), ("Collation", "VARCHAR(1)"),
            ("Cardinality", "INTEGER"), ("Sub_part", "INTEGER"),
            ("Packed", "VARCHAR(10)"), ("Null", "VARCHAR(3)"),
            ("Index_type", "VARCHAR(16)"), ("Comment", "VARCHAR(255)"),
            ("Index_comment", "VARCHAR(255)"))
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

    return self.expand_meta(meta), data

  def show_charset(self):
    meta = (("Charset", "VARCHAR(30)"), ("Description", "VARCHAR(60)"),
            ("Default collation", "VARCHAR(60)"), ("Maxlen", "INTEGER"))
    data = [("utf8", "UTF-8 Unicode", Charset.UTF8_GENERAL_CI.name.lower(), 3)]
    return self.expand_meta(meta), data

  def show_collation(self):
    meta = (("Collation", "VARCHAR(30)"), ("Charset", "VARCHAR(30)"),
            ("Id", "INTEGER"), ("Default", "VARCHAR(30)"),
            ("Compiled", "VARCHAR(30)"), ("Sortlen", "INTEGER"))
    data = [(Charset.UTF8_GENERAL_CI.name.lower(), "utf8",
             Charset.UTF8_GENERAL_CI, "Yes", "Yes", 1)]
    return self.expand_meta(meta), data

  def show_engines(self):
    meta = (("Engine", "VARCHAR(10)"), ("Support", "VARCHAR(10)"),
            ("Comment", "VARCHAR(80)"))
    data = [("SQLite", "DEFAULT", "Small. Fast. Reliable. Choose any three.")]
    return self.expand_meta(meta), data

  def show_table_status(self, name=None):
    meta = (("Name", "VARCHAR(64)"), ("Engine", "VARCHAR(10)"),
            ("Version", "INTEGER"), ("Row_format", "VARCHAR(10)"),
            ("Rows", "INTEGER"), ("Avg_row_length", "INTEGER"),
            ("Data_length", "INTEGER"), ("Max_data_length", "INTEGER"),
            ("Index_length", "INTEGER"), ("Data_free", "INTEGER"),
            ("Auto_increment", "INTEGER"), ("Create_time", "VARCHAR(19)"),
            ("Update_time", "VARCHAR(19)"), ("Check_time", "VARCHAR(19)"),
            ("Collation", "VARCHAR(32)"), ("Checksum", "INTEGER"),
            ("Create_options", "VARCHAR(255)"), ("Comment", "VARCHAR(80)"))
    data = []
    tables = [name] if name else self.get_tables()

    for table in tables:
      rows = self._count_rows(table)
      collation = Charset.UTF8_GENERAL_CI.name.lower()
      auto = self._next_id(table)
      data.append((table, "SQLite", 9, "Dynamic", rows, 0, 0, None, 0, 0,
                   auto, None, None, None, collation, None, "", ""))

    return self.expand_meta(meta), data

  def visible_type(self, name):
    field, length, decimals = self.internal_type(name)

    if field == FieldType.LONGLONG:
      return f"int({length})"
    elif field == FieldType.DECIMAL:
      return f"decimal({length},{decimals})"
    elif field == FieldType.DOUBLE:
      return f"double({length},{decimals})"
    elif field == FieldType.VAR_STRING:
      return f"varchar({length})"
    elif field == FieldType.DATETIME:
      return "datetime"
    elif field == FieldType.TIMESTAMP:
      return "timestamp"
    elif field == FieldType.BLOB:
      return "blob"
    else:
      return "text"

  def internal_type(self, name):
    if name is None:
      return FieldType.BLOB, 2 ** 24 - 1, 0

    name = name.upper()
    field = name
    length = 0
    decimals = 0

    results = match(r"(\w+2?)\((\d+),(\d+)\)", name)
    if results:
      field, length, decimals = results.groups()
    else:
      results = match(r"(\w+2?)\((\d+)\)", name)
      if results:
        field, length = results.groups()

    length = int(length)
    decimals = int(decimals)

    if "INT" in field:
      return FieldType.LONGLONG, 21, 0
    elif "DECIMAL" in field or "NUMERIC" in field:
     return FieldType.DECIMAL, length, decimals
    elif "FLOAT" in field or "DOUBLE" in field or "REAL" in field:
      if length == 0:
        length = 53
      if decimals + length > 53:
        length -= decimals
      return FieldType.DOUBLE, length, decimals
    elif "CHAR" in field:
      if length == 0:
        length = 2 ** 8 - 1
      return FieldType.VAR_STRING, length, 0
    elif "DATE" in field:
      return FieldType.DATETIME, 19, 0
    elif "STAMP" in field:
      return FieldType.TIMESTAMP, 19, 0
    elif "TEXT" in field:
      return FieldType.VAR_STRING, 2 ** 16 - 1, 0
    else:
      return FieldType.BLOB, 2 ** 24 - 1, 0

  def show_columns(self, name, full=False):
    meta = None
    data = []

    if not full:
      meta = (("Field", "VARCHAR(64)"), ("Type", "VARCHAR(40)"),
              ("Null", "VARCHAR(1)"), ("Key", "VARCHAR(3)"),
              ("Default", "VARCHAR(64)"), ("Extra", "VARCHAR(255)"))
    else:
      meta = (("Field", "VARCHAR(64)"), ("Type", "VARCHAR(40)"),
              ("Collation", "VARCHAR(40)"), ("Null", "VARCHAR(1)"),
              ("Key", "VARCHAR(3)"), ("Default", "VARCHAR(64)"),
              ("Extra", "VARCHAR(20)"), ("Privileges", "VARCHAR(80)"),
              ("Comment", "VARCHAR(255)"))

    columns = self._column_list(name)

    for column in columns:
      name = column["name"]
      key = "PRI" if column["primary"] else ""
      null = "YES" if column["nullable"] else "NO"
      extra = "auto_increment" if column["serial"] else ""
      default = column["default"]
      field = column["type"]
      collation = ""
      if field == "text" or "char" in field:
        collation = Charset.UTF8_GENERAL_CI.name.lower()

      if key == "" and column["index"]:
        key = "UNI" if column["unique"] else "MUL"

      if not full:
        data.append([name, field, null, key, default, extra])
      else:
        data.append([name, field, collation, null, key, default, extra, "", ""])

    return self.expand_meta(meta), data

  def _exectrace(self, cursor, sql, bindings):
    self._meta = cursor.getdescription()
    return True

  def expand_meta(self, meta):
    new_meta = []

    for name, field in meta:
      _type, length, decimals = self.internal_type(field)
      new_meta.append((name, field, _type, length, decimals))

    return new_meta

  def execute(self, query, params=None):
    results = self._execute(query, params)

    return self.expand_meta(self._meta), results.fetchall()
