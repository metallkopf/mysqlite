#!/usr/bin/env python

from socketserver import ThreadingTCPServer
from argparse import ArgumentParser
from server import Server


if __name__ == "__main__":
  parser = ArgumentParser()
  parser.add_argument("--path", required=True)
  parser.add_argument("--address", default="localhost")
  parser.add_argument("--port", default=3306)
  args = parser.parse_args()

  with ThreadingTCPServer((args.address, args.port), Server, False) as server:
    try:
      server.allow_reuse_address = True
      server.server_bind()
      server.server_activate()
      server.path = args.path

      server.serve_forever()
    except KeyboardInterrupt:
      server.shutdown()
