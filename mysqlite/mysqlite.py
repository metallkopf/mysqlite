#!/usr/bin/env python3

from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from logging import DEBUG, INFO, basicConfig, info
from os.path import isfile
from socketserver import ThreadingTCPServer
from sys import exit

from mysqlite import __version__
from mysqlite.server import Server


def main():
  parser = ArgumentParser(prog="mysqlite", add_help=False, allow_abbrev=False, formatter_class=ArgumentDefaultsHelpFormatter)
  parser.add_argument("--filename", help="Filename of the SQLite database")
  parser.add_argument("--address", default="localhost", help="IP address to bind to")
  parser.add_argument("--port", default=3306, type=int, help="Port number to use for connections")
  parser.add_argument("--debug", action="store_true", help="Print packets payload")
  parser.add_argument("--version", action="store_true", help="Version information")
  parser.add_argument("--help", action="store_true", help="This help")
  args = parser.parse_args()

  if args.version:
    print(f"MySQLite {__version__}")
    exit()
  elif args.help or args.filename is None or len(args.filename) == 0:
    parser.print_help()
    exit()
  elif not isfile(args.filename):
    raise FileNotFoundError(args.filename)

  level = DEBUG if args.debug else INFO
  basicConfig(format="%(threadName)s %(levelname).3s %(message)s", level=level)

  with ThreadingTCPServer((args.address, args.port), Server, False) as server:
    try:
      info(f"MySQLite {__version__}")
      info("STARTING...")
      server.allow_reuse_address = True
      server.server_bind()
      server.server_activate()
      server.filename = args.filename

      server.serve_forever()
    except KeyboardInterrupt:
      info("STOPPING...")
      server.shutdown()


if __name__ == "__main__":
  main()
