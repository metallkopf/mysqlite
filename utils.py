from struct import pack, unpack, calcsize
from io import BytesIO


def pack_string(value=None):
  if value is not None and len(value) > 0:
    return pack_varinteger(len(value)) + pack_fixedstring(value)
  else:
    return pack_padding()

def pack_resstring(value):
  if value is None:
    return pack_byte(0xfb)
  elif type(value) is not str:
    value = str(value)

  return pack_string(value)

def pack_nullstring(value=""):
  if len(value) > 0:
    return pack("%dsx" % len(value), value.encode())
  else:
    return pack_padding()

def pack_fixedstring(value="", length=1):
  return pack("%ds" % max(len(value), length), value.encode())

def pack_byte(value):
  return pack("<B", value)

def pack_integer(value):
  return pack("<H", value)

def pack_long(value):
  return pack("<I", value)

def pack_doublelong(value):
  return pack("<Q", value)

def pack_padding(times=1):
  return pack("x" * times)

def pack_varinteger(value):
  if value < 251:
    return pack_byte(value)
  elif value >= 251 and value < 2 ** 16:
    return pack_byte(0xfc) + pack_integer(value)
  elif value >= 2 ** 16 and value < 2 ** 24:
    return pack_byte(0xfd) + pack_long(value) >> 8
  else: #value >= 2 ** 24 and value < 2 ** 64
    return pack_byte(0xfe) + pack_doublelong(value)

def read_data(payload, fmt):
  data = payload.read(calcsize(fmt))
  return unpack(fmt, data)

def read_string(payload):
  buffer = BytesIO()

  while True:
    character = payload.read(1)

    if len(character) == 0 or ord(character) == 0:
      break

    buffer.write(character)

  return buffer.getvalue().decode("utf-8")

def read_varstring(payload):
  length = read_data(payload, "<B")[0]
  return read_data(payload, "%ds" % length)[0]
