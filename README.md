# rethinkdown
A RethinkDB implementation of the LevelDOWN API

###### Tested with RethinkDB drivers
* `rethinkdb`
* `rethinkdbdash`

#### Example (ES6)

```js
import rethinkdb from 'rethinkdb'
import rethinkdown from 'rethinkdown'

let down = rethinkdown(rethinkdb)
let db = down('rethinkdb://my.host.com/development/down')

db.open({ createIfMissing: true }, (error) => {
  db.put('1', 'one', (error) => {
    db.get('one', { asBuffer: false }, (error, value) => {
      console.log(value)
      // > one
      db.close((error) => {
        // ...
      })
    })
  })
})
```

#### API

Refer to the [`LevelDOWN`](https://github.com/level/leveldown) or [`AbstractLevelDOWN`](https://github.com/Level/abstract-leveldown) documentation for full API

A `RethinDOWN` instance needs to first be initialized with either a `rethinkdb` or `rethinkdbdash` driver

```js
import rethinkdbdash from 'rethinkdbdash'
import rethinkdown from 'rethinkdown'

let db = rethinkdown(rethinkdbdash)

```
---

##### rethinkdown ( `location` )

returns a new RethinkDOWN instance

**Parameters**

* `location` {[`String`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)} - A connection string for the rethinkdb database or a table name on the local rethinkdb database

**Returns** {[`RethinkDOWN`](https://github.com/bhoriuchi/rethinkdown)}

---

#### location

The location connection string takes the following format

`rethinkdb://[user:password@]host[:port]/[db/]table[?timeout=number&silent=boolean]`

* [`user`] : username for the database
* [`password`] : password for the user
* `host` : hostname of the database server (i.e. `localhost`)
* [`port=28015`] : non-default port database is listening on
* [`db=test`] : specific database to use
* `table` : table name to use as the `LevelDOWN` database
* [`timeout`] : optional timeout argument for connection
* [`silent`] : boolean option (`true` or `false`) for silent connection to `rethinkdbdash`

If a string not prefixed by `rethinkdb://` is supplied it will be assumed as the `tableName` and `rethinkdown` will attempt to connect to `rethinkdb://localhost:28015/test/tableName`

##### location Examples

```js
"rethinkdb://localhost/test/leveldb"
"rethinkdb://my.domain.com/test/leveldb"
"rethinkdb://my.domain.com/leveldb"
"rethinkdb://my.domain.com/test/leveldb?timeout=1000"
"rethinkdb://my.domain.com/test/leveldb?silent=true"
"rethinkdb://my.domain.com/test/leveldb?timeout=100&silent=true"
"rethinkdb://me:mypassword@my.domain.com/test/leveldb"
"leveldb"
```