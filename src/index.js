/**
 * RethinkDOWN
 * @description A RethinkDB implementation of the LevelDOWN API
 * @author Branden Horiuchi <bhoriuchi@gmail.com>
 */
import util from 'util'
import {
  AbstractLevelDOWN,
  AbstractIterator,
  AbstractChainedBatch
} from 'abstract-leveldown'

// defaults
const DEFAULT_RETHINKDB_PORT = 28015
const DEFAULT_RETHINKDB_HOST = 'localhost'
const DEFAULT_RETHINKDB_DB = 'test'

// regex
const TABLE_NAME_RX = /\W+/g

// property values
const PUT_OPERATION = 'put'
const DEL_OPERATION = 'del'
const PK = 'id'
const KEY = 'key'
const VALUE = 'value'

// Error messages
const ERR_DB_DNE = 'Database %s does not exist'
const ERR_INVALID_BATCH_OP = 'Invalid batch operation. Valid operations are "put" and "del"'
const ERR_INVALID_PARAM = 'Invalid parameter %s must be type %s with valid value'
const ERR_INVALID_PRIMARY_KEY = 'Database %s table %s does not have its primary key set ' +
  'to "' + PK + '" and cannot be used, please re-create the table with the option ' +
  '"{ primaryKey: \'' + PK + '\' }" or remove the table and use the "createIfMissing" option' +
  'in the open method'
const ERR_NO_DB_TABLE = 'No table found in location'
const ERR_NO_RETHINK_DRIVER = 'No RethinkDB driver was provided'
const ERR_NOT_FOUND = 'Key %s not found'
const ERR_TABLE_DNE = 'Table %s does not exist'
const ERR_TABLE_EXISTS = 'TableExists'
const ERR_REQUIRES_CALLBACK = '%s() requires a callback argument'

/**
 * Gets an error object, for rethinkdb errors use the msg field
 * @param error
 * @return {Error}
 */
function DOWNError (error) {
  if (error instanceof Error) {
    return error.msg
      ? new Error(error.msg)
      : error
  } else if (typeof error === 'object') {
    try {
      return new Error(JSON.stringify(error))
    } catch (err) {
      return new Error(String(error))
    }
  }
  return new Error(String(error))
}

/**
 * Ensures that the value is a buffer or string
 * @param value
 * @param ensureBuffer
 * @return {Buffer}
 */
function asBuffer(value, ensureBuffer = true) {
  return (ensureBuffer && !Buffer.isBuffer(value))
    ? new Buffer(value)
    : Buffer.isBuffer(value )
      ? value.toString()
      : value
}

/**
 * Chained Batch class
 * @extends AbstractChainedBatch
 */
class RethinkChainedBatch extends AbstractChainedBatch {
  /**
   * Creates a new RethinkChainedBatch
   * @param {object} db - rethinkdown instance
   */
  constructor (db) {
    super(db)
  }
}

/**
 * Rethink Iterator
 * @extends AbstractIterator
 */
class RethinkIterator extends AbstractIterator {
  /**
   * Creates a new Iterator
   * @param {object} db - RethinkDOWN instance
   * @param {object} [options]
   */
  constructor (db, options = {}) {
    super(db)
    let r = db.$r
    let query = db.singleTable ? db.$t.filter({ location: db.location }) : db.$t
    let KEY_FIELD = db.singleTable ? KEY : PK

    let { gt, gte, lt, lte, start, end, reverse, keys, values, limit, keyAsBuffer, valueAsBuffer } = options
    this._keyAsBuffer = keyAsBuffer !== false
    this._valueAsBuffer = valueAsBuffer !== false

    // logic ported from mongodown - https://github.com/watson/mongodown
    if (reverse) {
      if (start) query = query.filter(r.row(KEY_FIELD).le(start))
      if (end) query = query.filter(r.row(KEY_FIELD).ge(end))
      if (gt) query = query.filter(r.row(KEY_FIELD).lt(gt))
      if (gte) query = query.filter(r.row(KEY_FIELD).le(gte))
      if (lt) query = query.filter(r.row(KEY_FIELD).gt(lt))
      if (lte) query = query.filter(r.row(KEY_FIELD).ge(lte))
    } else {
      if (start) query = query.filter(r.row(KEY_FIELD).ge(start))
      if (end) query = query.filter(r.row(KEY_FIELD).le(end))
      if (gt) query = query.filter(r.row(KEY_FIELD).gt(gt))
      if (gte) query = query.filter(r.row(KEY_FIELD).ge(gte))
      if (lt) query = query.filter(r.row(KEY_FIELD).lt(lt))
      if (lte) query = query.filter(r.row(KEY_FIELD).le(lte))
    }

    // sort the results by key depending on reverse value
    query = query.orderBy(reverse ? r.desc(KEY_FIELD) : r.asc(KEY_FIELD))

    // set limit
    query = (typeof limit === 'number' && limit >= 0)
      ? query.limit(limit)
      : query

    // run query
    this._query = db.$sync
      ? query.run({ cursor: true })
      : query.run(db.$connection)
  }

  /**
   * Gets the next key in the iterator results
   * @callback callback
   * @return {*}
   * @private
   */
  _next (callback) {
    return this._query.then((cursor) => {
      try {
        cursor.next((error, row) => {
          if (error) {
            return (error.name === 'ReqlDriverError' && error.message === 'No more rows in the cursor.')
              ? callback()
              : callback(DOWNError(error))
          }

          let key = this.db.singleTable ? row.key : row[PK]
          let value = row.value

          key = asBuffer(key, this._keyAsBuffer)
          value = asBuffer(value, this._valueAsBuffer)
          return callback(null, key, value)
        })
      } catch (error) {
        return callback(DOWNError(error))
      }
    })
  }

  /**
   * Destroys the iterator and closes the cursor
   * @callback callback
   * @private
   */
  _end (callback) {
    try {
      return this._query.then((cursor) => {
        return cursor.close((error) => {
          return error
            ? callback(DOWNError(error))
            : callback()
        })
      }, (error) => {
        return callback(DOWNError(error))
      })
    } catch (error) {
      return callback(DOWNError(error))
    }
  }

  // not implemented in abstract?
  seek (key) {
    throw DOWNError('seek is not implemented')
  }
}

/**
 * RethinkDOWN class
 * @extends AbstractLevelDOWN
 */
class RethinkDOWN extends AbstractLevelDOWN {
  /**
   * Initializes the connector and parses the location/connection string
   * @param {string} location - connection string in the form rethinkdb://[<user>:<password>@]<host>[:<port>][/<db>]/<table>[?timeout=<timeout>]
   * @param {object} r - rethinkdb driver
   */
  constructor (location, r, db, options = {}) {
    super(location)

    // validate that the location is a string and replace any invalid characters with _
    if (typeof location !== 'string') throw DOWNError(util.format(ERR_INVALID_PARAM, 'location', 'String'))

    // check for single table
    this.singleTable = (typeof options.singleTable === 'string')
      ? options.singleTable
      : null
    delete options.singleTable

    // store rethinkdb driver and initialize connection
    this.$r = r
    this.$connection = null
    this.$t = null
    this.$db = db
    this.$table = this.singleTable || location.replace(TABLE_NAME_RX, '_')
    this.$sync = false
    this.$options = options
  }

  /**
   * opens a database connection and optionally creates the database and/or table
   * @param {object} [options]
   * @callback callback
   * @returns {*}
   * @private
   */
  _open (options, callback) {
    try {
      // support some of the open options
      let { createIfMissing, errorIfExists } = options
      createIfMissing = typeof createIfMissing === 'boolean'
        ? createIfMissing
        : true
      errorIfExists = typeof errorIfExists === 'boolean'
        ? errorIfExists
        : false

      // processes open options to create or throw error
      let processOptions = (r) => {
        return r.dbList()
          .contains(this.$db)
          .branch(
            r.db(this.$db)
              .tableList()
              .contains(this.$table)
              .branch(
                r.expr(errorIfExists)
                  .branch(
                    r.expr(this.singleTable).eq(null).branch(
                      r.error(ERR_TABLE_EXISTS),
                      r.db(this.$db)
                        .table(this.$table)
                        .filter({ location: this.location })
                        .count()
                        .ne(0)
                        .branch(
                          r.error(ERR_TABLE_EXISTS),
                          true
                        )
                    ),
                    r.db(this.$db).table(this.$table).config()('primary_key').eq(PK)
                      .branch(
                        true,
                        r.error(util.format(ERR_INVALID_PRIMARY_KEY, this.$db, this.$table))
                      )
                  ),
                r.expr(createIfMissing)
                  .branch(
                    r.db(this.$db).tableCreate(this.$table, { primaryKey: PK }),
                    r.error(util.format(ERR_TABLE_DNE, this.$table))
                  )
              ),
            r.expr(createIfMissing)
              .branch(
                r.dbCreate(this.$db).do(() => r.db(this.$db).tableCreate(this.$table, { primaryKey: PK })),
                r.error(util.format(ERR_DB_DNE, this.$db))
              )
          )
          .run(this.$connection, (error) => {
            if (error) return callback(DOWNError(error))
            this.$t = r.db(this.$db).table(this.$table)
            return callback()
          })
      }

      // check for standard rethinkdb driver
      if (typeof this.$r.connect === 'function') {
        this.$sync = false
        return this.$r.connect(this.$options, (error, connection) => {
          if (error) return callback(DOWNError(error))
          this.$connection = connection
          return processOptions(this.$r)
        })
      }

      // otherwise use synchronous driver (rethinkdbdash)
      this.$r = this.$r(this.$options)
      this.$sync = true
      this.$connection = {}
      return processOptions(this.$r)

    } catch (error) {
      return callback(DOWNError(error))
    }
  }

  /**
   * closes a database connection or cleans up the pool
   * @callback callback
   * @returns {*}
   * @private
   */
  _close (callback) {
    try {
      if (this.$connection && typeof this.$connection.close === 'function') {
        return this.$connection.close(callback)
      } else if (typeof this.$r.getPoolMaster === 'function') {
        this.$r.getPoolMaster().drain()
        return callback()
      }
    } catch (error) {
      return callback(DOWNError(error))
    }
  }

  /**
   * gets a value by key
   * @param {string|buffer} key
   * @param {object} [options]
   * @callback callback
   * @return {*}
   * @private
   */
  _get (key, options, callback) {
    try {
      let r = this.$r
      let { asBuffer } = options

      let query = this.singleTable
        ? this.$t.filter({ location: this.location, key }).nth(0)
        : this.$t.get(key)

      // get the key
      return query.default(null)
        .do((result) => {
          return result.eq(null)
            .branch(
              r.error(util.format(ERR_NOT_FOUND, key)),
              result(VALUE)
                .default(null)
                .do((value) => {
                  return r.expr([null, ''])
                    .contains(value)
                    .or(value.typeOf().eq('ARRAY').and(value.count().eq(0)))
                    .branch('', value)
                })
                .do((value) => {
                  return r.expr(asBuffer)
                    .branch(
                      value.coerceTo('BINARY'),
                      value.coerceTo('STRING')
                    )
                })
            )
        })
        .run(this.$connection, (error, value) => {
          return error
            ? callback(DOWNError(error))
            : callback(null, value)
        })
    } catch (error) {
      return callback(DOWNError(error))
    }
  }

  /**
   * reusable put mutation builder
   * @param key
   * @param value
   * @param opts
   * @returns {*}
   */
  $put (key, value, opts) {
    if (this.singleTable) {
      return this.$t.filter({ key, location: this.location })
        .coerceTo('ARRAY')
        .do((records) => {
          return records.count().ne(0).branch(
            this.$t.get(records.nth(0)(PK)).update({ value }, opts),
            this.$t.insert({ key, value, location: this.location }, Object.assign({ conflict: 'update' }, opts))
          )
        })
    }
    return this.$t.insert({ [PK]: key, value }, Object.assign({ conflict: 'update' }, opts))
  }

  /**
   * adds a value at a specific key
   * @param {string|buffer} key
   * @param {string|buffer} value
   * @param {object} [options]
   * @callback callback
   * @return {*}
   * @private
   */
  _put (key, value, options, callback) {
    try {
      let { sync } = options
      let durability = (sync === true)
        ? 'hard'
        : 'soft'

      // insert the value using durability an conflict to emulate sync option and avoid
      // branch statements for insert and update
      return this.$put(key, value, { durability })
        .pluck('errors', 'first_error')
        .run(this.$connection, (error, results) => {
          if (error) return callback(DOWNError(error))
          let { errors, first_error } = results
          if (errors && first_error) return callback(DOWNError(first_error))
          return callback()
        })
    } catch (error) {
      return callback(DOWNError(error))
    }
  }

  /**
   * reusable del mutation builder
   * @param key
   * @param opts
   * @returns {*}
   */
  $del (key, opts) {
    if (this.singleTable) {
      return this.$t.filter({ key, location: this.location })
        .coerceTo('ARRAY')
        .do((records) => {
          return records.count().ne(0).branch(
            this.$t.get(records.nth(0)(PK)).delete(opts),
            this.$r.error(util.format(ERR_NOT_FOUND, key)),
          )
        })
    }
    return this.$t.get(key)
      .eq(null)
      .branch(
        this.$r.error(util.format(ERR_NOT_FOUND, key)),
        this.$t.get(key).delete(opts)
      )
  }

  /**
   * Deletes a key
   * @param {string|buffer} key
   * @param {object} [options]
   * @callback callback
   * @return {*}
   * @private
   */
  _del (key, options, callback) {
    try {
      let { sync } = options
      let durability = (sync === true)
        ? 'hard'
        : 'soft'

      // delete the record
      return this.$del(key, { durability })
        .pluck('errors', 'first_error')
        .run(this.$connection, (error, results) => {
          if (error) return callback(DOWNError(error))
          let { errors, first_error } = results
          if (errors && first_error) return callback(DOWNError(first_error))
          return callback()
        })
    } catch (error) {
      return callback(DOWNError(error))
    }
  }

  /**
   * Performs batch operations of put and/or delete
   * @param {array} operations
   * @param {object} [options]
   * @callback callback
   * @private
   */
  _batch (operations, options, callback) {
    try {
      let ops = []
      let { sync } = options
      let durability = (sync === true)
        ? 'hard'
        : 'soft'

      // validate the operations list
      for (const operation of operations) {
        let { type, key, value } = operation

        // determine the operation type and create an operation object
        switch (type) {
          case PUT_OPERATION:
            // coerce the value into a valid value
            value = this._serializeValue(value)
            ops.push({ type: PUT_OPERATION, key, value })
            break

          case DEL_OPERATION:
            ops.push({ type: DEL_OPERATION, key })
            break

          default:
            return callback(DOWNError(ERR_INVALID_BATCH_OP))
        }
      }

      // perform operations
      return this.$r.expr(ops).forEach((op) => {
        return op('type').eq(PUT_OPERATION)
          .branch(
            this.$put(op(KEY), op(VALUE), { durability }),
            this.$del(op(KEY), { durability })
          )
      })
        .pluck('errors', 'first_error')
        .run(this.$connection, (error, results) => {
          if (error) return callback(DOWNError(error))
          let { errors, first_error } = results
          if (errors && first_error) return callback(DOWNError(first_error))
          return callback()
        })
    } catch (error) {
      return callback(DOWNError(error))
    }
  }

  /**
   * Returns a new chained batch
   * @return {RethinkChainedBatch}
   * @private
   */
  _chainedBatch () {
    return new RethinkChainedBatch(this)
  }

  /**
   * Gets count of records, doesnt really apply/work with rethinkdb
   * @param {string|buffer} start
   * @param {string|buffer} end
   * @callback callback
   * @return {number}
   * @private
   */
  _approximateSize (start, end, callback) {
    try {
      return this.$t.filter((record) => record.ge(start).and(record.le(end)))
        .count()
        .run(this.$connection, (error, size) => {
          if (error) return callback(DOWNError(error))
          return callback(null, size)
        })
    } catch (error) {
      return callback(DOWNError(error))
    }
  }

  /**
   * Returns a new iterator
   * @param options
   * @return {RethinkIterator}
   * @private
   */
  _iterator (options) {
    return new RethinkIterator(this, options)
  }
}

/**
 * RethinkDOWN
 * @param {object} r - RethinkDB driver
 * @return {DOWN}
 */
export default function (r, db, options = {}) {
  if (typeof db === 'object') {
    options = db
    db = DEFAULT_RETHINKDB_DB
  }

  // check for driver and set db
  if (!r) throw DOWNError(ERR_NO_RETHINK_DRIVER)
  db = db || DEFAULT_RETHINKDB_DB

  /**
   *
   * @param {string} location - connection string in the form rethinkdb://[<user>:<password>@]<host>[:<port>][/<db>]/<table>[?timeout=<timeout>]
   * @return {RethinkDOWN}
   * @constructor
   */
  let DOWN = function (location) {
    return new RethinkDOWN(location, r, db, options)
  }

  /**
   * Connects to the database and returns the cursor, connection, and parsed location
   * @param {string} location - connection string
   * @callback callback
   * @return {*}
   */
  let connect = (callback) => {
    if (typeof callback !== 'function') throw DOWNError(util.format(ERR_REQUIRES_CALLBACK, 'connect'))

    try {
      // async connection
      if (typeof r.connect === 'function') {
        return r.connect(options, (error, connection) => {
          if (error) return callback(DOWNError(error))
          return callback(null, r, connection)
        })
      }

      // sync connection
      r = r(options)
      return callback(null, r, {})

    } catch (error) {
      return callback(DOWNError(error))
    }
  }

  /**
   * Destroys the table specified by the location
   * @param {string} location - connection string
   * @callback callback
   * @return {*}
   */
  DOWN.destroy = (location, callback) => {
    if (typeof callback !== 'function') throw DOWNError(util.format(ERR_REQUIRES_CALLBACK, 'destroy'))
    if (typeof location !== 'string') throw DOWNError(util.format(ERR_INVALID_PARAM, 'location', 'String'))
    try {
      let table = location.replace(TABLE_NAME_RX, '_')
      return connect((error, cursor, connection) => {
        if (error) return callback(DOWNError(error))

        return cursor.db(db)
          .tableDrop(table)
          .run(connection, (error) => {
            if (error ) callback(DOWNError(error))
            return callback()
          })
      })
    } catch (error) {
      return callback(DOWNError(error))
    }
  }

  /**
   * Destroys a singlemode database
   * @param {string} location - connection string
   * @callback callback
   * @return {*}
   */
  DOWN.destroySingle = (singleTable, location, callback) => {
    if (typeof callback !== 'function') throw DOWNError(util.format(ERR_REQUIRES_CALLBACK, 'destroy'))
    if (typeof singleTable !== 'string') throw DOWNError(util.format(ERR_INVALID_PARAM, 'singleTable', 'String'))
    if (typeof location !== 'string') throw DOWNError(util.format(ERR_INVALID_PARAM, 'location', 'String'))
    try {
      return connect((error, cursor, connection) => {
        if (error) return callback(DOWNError(error))

        return cursor.db(db)
          .table(singleTable)
          .filter({ location })
          .delete()
          .run(connection, (error) => {
            if (error ) callback(DOWNError(error))
            return callback()
          })
      })
    } catch (error) {
      return callback(DOWNError(error))
    }
  }

  DOWN.repair = (location, callback) => {
    if (typeof callback !== 'function') throw DOWNError(util.format(ERR_REQUIRES_CALLBACK, 'repair'))
    return callback(DOWNError('repair not implemented'))
  }

  // return the creation method
  return DOWN
}