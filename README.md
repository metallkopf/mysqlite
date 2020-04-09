# MySQLite
MySQLite is an application that exposes a SQLite database as read-only over MySQL Protocol 4.1

 >**WARNING**: the server accepts all connections, there is not authentication method implemented, handle with care

## Install
```bash
# From source
pipenv install git+https://github.com/metallkopf/mysqlite.git@master#egg=mysqlite

# Wheels
pipenv install https://github.com/metallkopf/mysqlite/releases/download/0.1.0/mysqlite-0.1.0-py3-none-any.whl
```

## Run
```bash
pipenv run mysqlite --filename DATABASE.SQLITE
```

## Usage
```bash
mysqlite --help
```
```
usage: mysqlite [--filename FILENAME] [--address ADDRESS] [--port PORT] [--debug] [--version] [--help]

optional arguments:
  --filename FILENAME  Path of the SQLite database
  --address ADDRESS    IP address to bind to (default: localhost)
  --port PORT          Port number to use for connections (default: 3306)
  --debug              Print packets payload (default: False)
  --version            Version information
  --help               This help
```

## TODO (in no particular order)
* improve command support
* return more accurate data types
* implement authentication?
* multiples databases?

## License
[GPLv2](https://www.gnu.org/licenses/gpl-2.0.html)
