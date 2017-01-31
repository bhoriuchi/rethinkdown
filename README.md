# rethinkdown
A RethinkDB implementation of the LevelDOWN API

###### Tested with RethinkDB drivers
* `rethinkdb`
* `rethinkdbdash`

#### Example (ES6)

```js
import r from 'rethinkdb'
import rethinkdown from 'rethinkdown'

const database = 'test'
const options = { host: 'db.myserver.com' }
const table = 'mydbtable'

let db = rethinkdown(r, database, options)(table)

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

---

##### RethinkDOWN ( `driver`, [`database`], [`connectOptions`] )

A `RethinDOWN` instance needs to first be initialized with either a `rethinkdb` or `rethinkdbdash` driver, optional database name, and optional connection options

**Parameters**

* `driver` {[`rethinkdb`](https://github.com/rethinkdb/rethinkdb)|[`rethinkdbdash`](https://www.npmjs.com/package/rethinkdbdash)} - Supported RethinkDB driver
* [`database="test"`] {[`String`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)} - Database name to use
* [`connectOptions`] {[`Object`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object)} - A rethinkdb or rethinkdbdash [connect](https://rethinkdb.com/api/javascript/connect/) options object
* [`connectOptions.singleTable`] {[`String`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)} - Single mode table name
**Returns** {[`rethinkdown`](#rethinkdown)} - Instance of `rethinkdown` that can be passed a location/table

###### Example

```js
import r from 'rethinkdbdash'
import RethinkDOWN from 'rethinkdown'

let rethinkdown = RethinkDOWN(r, 'test', { silent: true })
let db = rethinkdown('myleveldb')
```
---

##### rethinkdown ( `location` )

returns a new RethinkDOWN instance

**Parameters**

* `location` {[`String`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)} - table name or record filter when using `single` mode

**Returns** {[`RethinkDOWN`](https://github.com/bhoriuchi/rethinkdown)}

---

#### location

The location should be the name of the table that should be used as the LevelDB store. All `non-alphanumeric` and `_` characters will be replaced with `_`

#### Single-Mode `since v0.3.0`

Single mode allows the same table to be used for all `level` databases. The table name specified by `connectOptions.singleTable` will store all data and the `location` will be added as a value in each record so that multiple databases can be stored in the same table.
