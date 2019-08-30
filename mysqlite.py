#!/usr/bin/env python

from socketserver import ThreadingTCPServer
from server import Server


if __name__ == "__main__":
  server_address = ("localhost", 3306)

  with ThreadingTCPServer(server_address, Server, False) as server:
    try:
      server.allow_reuse_address = True
      server.server_bind()
      server.server_activate()

      server.serve_forever()
    except KeyboardInterrupt:
      server.shutdown()
