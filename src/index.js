import util from 'util'
import url from 'url'
import {
  AbstractLevelDOWN,
  AbstractIterator,
  AbstractChainedBatch
} from 'abstract-leveldown'

const DEFAULT_RETHINKDB_PORT = 28015
const DEFAULT_RETHINKDB_HOST = 'localhost'
const DEFAULT_RETHINKDB_DB = 'test'
const TIMEOUT_RX = /(.*)(timeout=)(\d+)(.*)/i
const PUT_OPERATION = 'put'
const DEL_OPERATION = 'del'

// Error messages
const ERR_DB_DNE = 'Database %s does not exist'
const ERR_INVALID_KEY = 'InvalidKey'
const ERR_INVALID_LOCATION = 'Invalid location'
const ERR_INVALID_BATCH_OP = 'Invalid batch operation. Valid operations are "put" and "del"'
const ERR_INVALID_PARAM = 'Invalid parameter %s must be type %s with valid value'
const ERR_INVALID_PRIMARY_KEY = 'Database %s table %s does not have its primary key set ' +
  'to "key" and cannot be used, please re-create the table with the option ' +
  '"{ primaryKey: \'key\' }" or remove the table and use the "createIfMissing" option' +
  'in the open method'
const ERR_NO_DB_HOST = 'No hostname found in location'
const ERR_NO_DB_TABLE = 'No table found in location'
const ERR_NO_RETHINK_DRIVER = 'No RethinkDB driver was provided'
const ERR_NOT_FOUND = 'Key %s not found'
const ERR_TABLE_DNE = 'Table %s does not exist'
const ERR_TABLE_EXISTS = 'TableExists'
const ERR_REQUIRES_CALLBACK = '%s() requires a callback argument'

/**
 * Determines if an object is empty, which includes null, undefined, '', [], {}, and Buffer(0)
 * @param {*} obj
 * @return {boolean}
 */
function isEmpty (obj) {
  if (obj === null
    || obj === undefined
    || obj === ''
    || (Array.isArray(obj) && !obj.length)
    || (obj instanceof Buffer && !obj.length)
    || (typeof obj === 'object' && !Object.keys(obj).length)) {
    return true
  }
  return false
}

/**
 * Coerces the key to a valid value or returns an error object if the key is invalid
 * @param {string|buffer} key
 * @return {string|buffer|Error}
 */
function coerceKey (key) {
  if (isEmpty(key)) return new Error(ERR_INVALID_KEY)
  if (!(key instanceof Buffer) && typeof key !== 'string') key = String(key)
  if (isEmpty(key)) return new Error(ERR_INVALID_KEY)
  return key
}

/**
 * Parses the location string for database connection properties
 * @param {string} location - connection string or table name
 * @return {object}
 */
function parseLocation (location) {
  let options = {}
  let { protocol, auth, hostname, port, query, pathname } = url.parse(location)

  // check for location
  if (!protocol) {
    hostname = DEFAULT_RETHINKDB_HOST
    pathname = `/${location}`
  }

  // add host
  if (!hostname) return new Error(ERR_NO_DB_HOST)
  else options.host = hostname

  // add port
  options.port = port
    ? port
    : DEFAULT_RETHINKDB_PORT

  // add db and table
  let [ , db, table ] = (pathname ? pathname : '').split('/')
  if (!db) return new Error(ERR_NO_DB_TABLE)
  if (!table) {
    table = db
    db = DEFAULT_RETHINKDB_DB
  }
  options.db = db
  options.table = table

  // add authentication
  let [ user, password ] = (auth ? auth : '').split(':')
  if (user) options.user = user
  if (password) options.password = password

  // add timeout
  if (query && query.match(TIMEOUT_RX)) {
    options.timeout = Number(query.replace(TIMEOUT_RX, '$3'))
  }
  return options
}

class RethinkChainedBatch extends AbstractChainedBatch {
  constructor () {
    super()
  }

  _put () {

  }

  _del () {

  }

  _clear () {

  }

  _write () {

  }

  _serializeKey () {

  }

  _serializeValue () {

  }
}

class RethinkIterator extends AbstractIterator {
  constructor (db, options) {
    super(db)
  }

  _next () {

  }

  _end () {

  }

  // not implemented in abstract?
  seek () {

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
  constructor (location, r) {
    if (typeof location !== 'string') throw new Error(ERR_INVALID_LOCATION)
    super(location)

    // store rethinkdb driver and initialize connection
    this.$r = r
    this.$connection = null
    this.$t = null

    // parse the connection string for connection options
    let opts = parseLocation(location)
    if (opts instanceof Error) throw opts

    let { host, port, db, table, user, password, timeout } = opts

    this.$connectOptions = { host, port, db, user, password, timeout }
    this.$table = table
  }

  /**
   * opens a database connection and optionally creates the database and/or table
   * @param {object} [options]
   * @callback callback
   * @returns {*}
   * @private
   */
  _open (options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    if (typeof callback !== 'function') throw new Error(util.format(ERR_REQUIRES_CALLBACK, 'open'))

    try {
      // support some of the open options
      let r = this.$r
      let { db } = this.$connectOptions
      let { createIfMissing, errorIfExists } = options
      createIfMissing = typeof createIfMissing === 'boolean'
        ? createIfMissing
        : true
      errorIfExists = typeof errorIfExists === 'boolean'
        ? errorIfExists
        : false

      // processes open options to create or throw error
      let processOptions = () => {
        return r.dbList()
          .contains(db)
          .branch(
            r.db(db)
              .tableList()
              .contains(this.$table)
              .branch(
                r.expr(errorIfExists)
                  .branch(
                    r.error(ERR_TABLE_EXISTS),
                    r.db(db).table(this.$table).config()('primary_key').eq('key')
                      .branch(
                        true,
                        r.error(util.format(ERR_INVALID_PRIMARY_KEY, db, this.$table))
                      )
                  ),
                r.expr(createIfMissing)
                  .branch(
                    r.db(db).tableCreate(this.$table, { primaryKey: 'key' }),
                    r.error(util.format(ERR_TABLE_DNE, this.$table))
                  )
              ),
            r.expr(createIfMissing)
              .branch(
                r.dbCreate(db).do(() => r.db(db).tableCreate(this.$table, { primaryKey: 'key' })),
                r.error(util.format(ERR_DB_DNE, db))
              )
          )
          .run(this.$connection)
          .then(() => {
            this.$t = r.db(db).table(this.$table)
            return callback()
          })
          .catch((error) => {
            callback(error.msg ? new Error(error.msg) : error)
          })
      }

      // check for standard rethinkdb driver
      if (typeof r.connect === 'function') {
        return r.connect(this.$connectOptions, (err, connection) => {
          if (err) return callback(err)
          this.$connection = connection
          return processOptions()
        })
      }

      // otherwise use synchronous driver
      this.$r = r(this.$connectOptions)
      return processOptions()
    } catch (error) {
      return callback(error.msg ? new Error(error.msg) : error)
    }
  }

  /**
   * closes a database connection or cleans up the pool
   * @callback callback
   * @returns {*}
   * @private
   */
  _close (callback) {
    if (typeof callback !== 'function') throw new Error(util.format(ERR_REQUIRES_CALLBACK, 'close'))

    try {
      if (this.$connection && typeof this.$connection.close === 'function') {
        return this.$connection.close(callback)
      } else if (typeof this.$r.getPoolMaster === 'function') {
        this.$r.getPoolMaster().drain()
        return callback()
      }
    } catch (error) {
      return callback(error.msg ? new Error(error.msg) : error)
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
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    if (typeof callback !== 'function') throw new Error(util.format(ERR_REQUIRES_CALLBACK, 'get'))

    try {
      let r = this.$r
      let table = this.$t
      let { asBuffer } = options

      asBuffer = (typeof asBuffer === 'boolean')
        ? asBuffer
        : true

      key = coerceKey(key)
      if (key instanceof Error) return callback(key)

      return table.get(key)
        .default(null)
        .do((result) => {
          return result.eq(null)
            .branch(
              r.error(util.format(ERR_NOT_FOUND, key)),
              result('value')
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
                      value
                    )
                })
            )
        })
        .run(this.$connection)
        .then((value) => {
          return callback(null, value)
        })
        .catch((error) => {
          return callback(error.msg ? new Error(error.msg) : error)
        })

    } catch (error) {
      return callback(error.msg ? new Error(error.msg) : error)
    }
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
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    if (typeof callback !== 'function') throw new Error(util.format(ERR_REQUIRES_CALLBACK, 'put'))

    try {
      let { sync } = options
      let conflict = 'update'
      let returnChanges = true
      let durability = (sync === true)
        ? 'hard'
        : 'soft'

      // coerce the key into a valid value or throw error
      key = coerceKey(key)
      if (key instanceof Error) return callback(key)

      // coerce the value into a valid value
      value = (isEmpty(value) || isEmpty(String(value)))
        ? ''
        : (value instanceof Buffer)
          ? value
          : String(value)

      // insert the value using durability an conflict to emulate sync option and avoid
      // branch statements for insert and update
      return this.$t.insert({ key, value }, { conflict, durability, returnChanges })
        .pluck('errors', 'first_error')
        .run(this.$connection)
        .then((results) => {
          let { errors, first_error } = results
          if (errors && first_error) return callback(new Error(first_error))
          return callback()
        })
        .catch((error) => {
          return callback(error.msg ? new Error(error.msg) : error)
        })
    } catch (error) {
      return callback(error.msg ? new Error(error.msg) : error)
    }
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
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    if (typeof callback !== 'function') throw new Error(util.format(ERR_REQUIRES_CALLBACK, 'del'))

    try {
      let { sync } = options
      let returnChanges = true
      let durability = (sync === true)
        ? 'hard'
        : 'soft'

      // coerce the key into a valid value or throw error
      key = coerceKey(key)
      if (key instanceof Error) return callback(key)

      return this.$t.get(key)
        .eq(null)
        .branch(
          this.$r.error(util.format(ERR_NOT_FOUND, key)),
          this.$t.get(key).delete({ durability, returnChanges })
        )
        .pluck('errors', 'first_error')
        .run(this.$connection)
        .then((results) => {
          let { errors, first_error } = results
          if (errors && first_error) return callback(new Error(first_error))
          return callback()
        })
        .catch((error) => {
          return callback(error.msg ? new Error(error.msg) : error)
        })

    } catch (error) {
      return callback(error.msg ? new Error(error.msg) : error)
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
    // support chained batch
    if (!operations) return new RethinkChaindBatch()

    // standard use
    if (typeof options === 'function') {
      callback = options
      options = {}
    }

    try {
      if (typeof callback !== 'function') {
        throw new Error(util.format(ERR_REQUIRES_CALLBACK, 'batch'))
      } else if (!Array.isArray(operations) || operations.length === 0) {
        throw new Error(util.format(ERR_INVALID_PARAM, 'operations', 'array'))
      }

      let ops = []
      let { sync } = options
      let returnChanges = true
      let conflict = 'update'
      let durability = (sync === true)
        ? 'hard'
        : 'soft'

      // validate the operations list
      for (const operation of operations) {
        let { type, key, value } = operation

        // validate the key
        key = coerceKey(key)
        if (key instanceof Error) return callback(key)

        // determine the operation type and create an operation object
        switch (type) {
          case PUT_OPERATION:
            // coerce the value into a valid value
            value = (isEmpty(value) || isEmpty(String(value)))
              ? ''
              : (value instanceof Buffer)
                ? value
                : String(value)
            ops.push({ type: PUT_OPERATION, key, value })
            break

          case DEL_OPERATION:
            ops.push({ type: DEL_OPERATION, key })
            break

          default:
            return callback(new Error(ERR_INVALID_BATCH_OP))
        }
      }

      // perform operations
      return this.$r.expr(ops).forEach((op) => {
        return op('type').eq(PUT_OPERATION)
          .branch(
            this.$t.insert({ key: op('key'), value: op('value') }, { conflict, durability, returnChanges }),
            this.$t.get(op('key'))
              .eq(null)
              .branch(
                this.$r.error(util.format(ERR_NOT_FOUND, op('key'))),
                this.$t.get(op('key')).delete({ durability, returnChanges })
              )
          )
      })
        .pluck('errors', 'first_error')
        .run(this.$connection)
        .then((results) => {
          let { errors, first_error } = results
          if (errors && first_error) return callback(new Error(first_error))
          return callback()
        })
        .catch((error) => {
          return callback(error.msg ? new Error(error.msg) : error)
        })

    } catch (error) {
      return callback(error.msg ? new Error(error.msg) : error)
    }
  }

  _chainedBatch () {

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
    if (typeof callback !== 'function') {
      throw new Error(util.format(ERR_REQUIRES_CALLBACK, 'approximateSize'))
    } else if (typeof start !== 'string' && !(start instanceof Buffer)) {
      return callback(new Error(util.format(ERR_INVALID_PARAM, 'start', 'string or Buffer')))
    } else if (typeof end !== 'string' && !(end instanceof Buffer)) {
      return callback(new Error(util.format(ERR_INVALID_PARAM, 'end', 'string or Buffer')))
    }

    try {
      return this.$r.filter((record) => record.ge(start).and(record.le(end)))
        .count()
        .run(this.$connection)
        .then((size) => {
          return callback(null, size)
        })
        .catch((error) => {
          return callback(error.msg ? new Error(error.msg) : error)
        })
    } catch (error) {
      return callback(error.msg ? new Error(error.msg) : error)
    }
  }

  _serializeKey () {

  }

  _serializeValue () {

  }

  _iterator (options) {
    return new RethinkIterator(this, options)
  }

  // not implemented in abstract?
  getProperty () {

  }
}

/**
 * RethinkDOWN
 * @param {object} r - RethinkDB driver
 * @return {DOWN}
 */
export default function (r) {
  if (!r) throw new Error(ERR_NO_RETHINK_DRIVER)

  /**
   *
   * @param {string} location - connection string in the form rethinkdb://[<user>:<password>@]<host>[:<port>][/<db>]/<table>[?timeout=<timeout>]
   * @return {RethinkDOWN}
   * @constructor
   */
  let DOWN = function (location) {
    return new RethinkDOWN(location, r)
  }

  DOWN.destroy = () => true
  DOWN.repair = () => true

  // return the creation method
  return DOWN
}