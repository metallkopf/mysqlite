#!/usr/bin/env python3

from argparse import ArgumentParser
from logging import DEBUG, INFO, basicConfig, info
from os.path import isfile
from socketserver import ThreadingTCPServer

from server import Server


if __name__ == "__main__":
  parser = ArgumentParser()
  parser.add_argument("--path", required=True)
  parser.add_argument("--address", default="localhost")
  parser.add_argument("--port", default=3306)
  parser.add_argument("--debug", action="store_true")
  args = parser.parse_args()

  if not isfile(args.path):
    raise FileNotFoundError(args.path)

  level = DEBUG if args.debug else INFO
  basicConfig(format="%(threadName)s %(levelname).3s %(message)s", level=level)

  with ThreadingTCPServer((args.address, args.port), Server, False) as server:
    try:
      info("STARTING...")
      server.allow_reuse_address = True
      server.server_bind()
      server.server_activate()
      server.path = args.path

      server.serve_forever()
    except KeyboardInterrupt:
      info("STOPPING...")
      server.shutdown()
