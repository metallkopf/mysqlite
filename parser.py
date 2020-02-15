from re import IGNORECASE, MULTILINE, match


STATEMENTS = {
  "show_character_set": r"^SHOW\s+CHARACTER\s+SET(?:\s+LIKE\s+'(?P<pattern>[^']+)')?$",
  "show_collation": r"^SHOW\s+COLLATION(?:\s+LIKE\s+'(?P<pattern>[^']+)')?$",
  "show_columns": r"^SHOW(?:\s+(?P<modifier>FULL))?\s+COLUMNS\s+FROM\s+(?P<table>\w+|`.*?`)(?:\s+FROM\s+(?P<database>\w+|`[^`]+`))?(?:\s+LIKE\s+'(?P<pattern>[^']+)')?$",
  "show_create_database": r"^SHOW\s+CREATE\s+DATABASE\s+(?P<name>\w+|`[^`]+`)$",
  "show_create_table": r"^SHOW\s+CREATE\s+TABLE\s+(?P<name>\w+|`[^`]+`)$",
  "show_databases": r"^SHOW\s+DATABASES(?:\s+LIKE\s+'(?P<pattern>[^']+)')?$",
  "show_engines": r"^SHOW(?:\s+(?P<modifier>STORAGE))?\s+ENGINES$",
  "show_index": r"^SHOW\s+INDEX\s+FROM\s+(?P<table>\w+|`.*?`)(?:\s+FROM\s+(?P<database>\w+|`[^`]+`))?$",
  "show_processlist": r"^SHOW(?:\s+(?P<modifier>FULL))?\s+PROCESSLIST$",
  "show_table_status": r"^SHOW\s+TABLE\s+STATUS(?:\s+FROM\s+(?P<database>\w+|`[^`]+`))?(?:\s+LIKE\s+'(?P<pattern>[^']+)')?$",
  "show_tables": r"^SHOW\s+TABLES(?:\s+FROM\s+(?P<database>\w+|`[^`]+`))?(?:\s+LIKE\s+'(?P<pattern>[^']+)')?$",
  "show_status": r"^SHOW\s+STATUS(?:\s+LIKE\s+'(?P<pattern>[^']+)')?$",
  "show_variables": r"^SHOW(?:\s+(?P<modifier>GLOBAL|SESSION))?\s+VARIABLES(?:\s+LIKE\s+'(?P<pattern>[^']+)')?$",
  # describe
  # explain
  "help": r"^HELP\s+'(?P<search>[^']+)'$",
  "use": r"^USE\s+(?P<database>\w+|`[^`]+`)$",
}


def guess_statement(query):
  for function, pattern in STATEMENTS.items():
    results = match(pattern, query, IGNORECASE | MULTILINE)
    if results:
      return function, results.groupdict()
  else:
    return None, None
