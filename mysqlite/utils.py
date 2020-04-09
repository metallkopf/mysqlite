from io import BytesIO
from struct import calcsize, pack, unpack


def pack_string(value=None):
  if value is not None and len(value) > 0:
    if not isinstance(value, bytes):
      value = value.encode()
    return pack_varinteger(len(value)) + pack_fixedstring(value)
  else:
    return pack_padding()


def pack_resstring(value):
  if value is None:
    return pack_byte(0xfb)
  elif type(value) not in [str, bytes]:
    value = str(value)

  return pack_string(value)


def pack_nullstring(value=""):
  if len(value) > 0:
    return pack_fixedstring(value) + b"\0"
  else:
    return pack_padding()


def pack_fixedstring(value="", length=1):
  if not isinstance(value, bytes):
    value = value.encode()
  return pack("%ds" % max(len(value), length), value)


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
    return pack_byte(0xfd) + pack_long(value)[:-1]
  else:  # value >= 2 ** 24 and value < 2 ** 64
    return pack_byte(0xfe) + pack_doublelong(value)


def pack_header(length, number):
  return pack_long(length)[:-1] + pack_byte(number)


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
  return read_data(payload, f"{length}s")[0]


def read_header(payload):
  return unpack("<I", payload.read(3) + b"\0")[0], unpack("<B", payload.read(1))[0]
