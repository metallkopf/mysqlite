# MySQLite
MySQLite is an application that exposes a SQLite database as read-only over MySQL Protocol 4.1

 >**WARNING**: the server accepts all connections, there is not authentication method implemented, handle with care

## Installation
```bash
$ curl https://github.com/metallkopf/mysqlite/archive/master.tar.gz -o mysqlite-master.tar.gz
$ tar zxf mysqlite-master.tar.gz
$ cd mysqlite-master
$ pipenv install
```

## Usage
```bash
$ pipenv run python mysqlite.py --help
usage: mysqlite.py [--help] --path PATH [--address ADDRESS] [--port PORT] [--debug]

optional arguments:
  --path PATH
  --address ADDRESS (default: localhost)
  --port PORT (default: 3306)
  --debug
```

## TODO (in no particular order)
* improve command support
* return more accurate data types
* implement authentication?

## License
[GPLv2](https://www.gnu.org/licenses/gpl-2.0.html)
