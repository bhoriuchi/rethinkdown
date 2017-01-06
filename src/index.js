import util from 'util'
import url from 'url'
import {
  AbstractLevelDOWN,
  AbstractIterator,
  AbstractChainedBatch
} from 'abstract-leveldown'

const DEFAULT_RETHINKDB_PORT = 28015
const DEFAULT_RETHINKDB_DB = 'test'
const TIMEOUT_RX = /(.*)(timeout=)(\d+)(.*)/i

// Error messages
const ERR_NOT_FOUND = 'NotFound'
const ERR_INVALID_KEY = 'InvalidKey'
const ERR_TABLE_EXISTS = 'TableExists'
const ERR_TABLE_NOT_EXISTS = 'TableNotExists'


const ERR_COULD_NOT_CREATE_DB = 'Could not create new Database instance'
const ERR_REQUIRES_CALLBACK = '%s() requires a callback argument'

// checks if the object is empty
function isEmpty (obj) {
  if (obj === null
    || obj === undefined
    || obj === ''
    || (Array.isArray(obj) && !obj.length)
    || (obj instanceof Buffer && !obj.length)) {
    return true
  }
  return false
}

// coerces the key to a valid format
function coerceKey (key) {
  if (isEmpty(key)) return new Error(ERR_INVALID_KEY)
  if (!(key instanceof Buffer) && typeof key !== 'string') key = String(key)
  if (isEmpty(key)) return new Error(ERR_INVALID_KEY)
  return key
}

// parses the location as a connection string for database connection info
function parseLocation (location) {
  let options = {}
  let { auth, hostname, port, query, pathname } = url.parse(location)

  // add host
  if (!hostname) return new Error('no hostname specified in connection string')
  else options.host = hostname

  // add port
  options.port = port
    ? port
    : DEFAULT_RETHINKDB_PORT

  // add db and table
  let [ , db, table ] = (pathname ? pathname : '').split('/')
  if (!db) return new Error('no database or table specified in connection string')
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
    if (typeof location !== 'string') {
      throw new Error('location must be in connection string format (i.e. rethinkdb://localhost/test/leveldown)')
    }
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
   * @param {object} options
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
                        r.error('database ' + db + ' table ' + this.$table + ' does not have its primary key set ' +
                          'to "key" and cannot be used, please re-create the table with the option ' +
                          '"{ primaryKey: \'key\' }" or remove the table and use the "createIfMissing" option' +
                          'in the open method')
                      )
                  ),
                r.expr(createIfMissing)
                  .branch(
                    r.db(db).tableCreate(this.$table, { primaryKey: 'key' }),
                    r.error(`table ${this.table} does not exist`)
                  )
              ),
            r.expr(createIfMissing)
              .branch(
                r.dbCreate(db).do(() => r.db(db).tableCreate(this.$table, { primaryKey: 'key' })),
                r.error(`database ${db} does not exist`)
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
    } catch (err) {
      return callback(err)
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
    } catch (err) {
      return callback(err)
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
              r.error('NotFound'),
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

    } catch (err) {
      return callback(err)
    }
  }

  /**
   * adds a value at a specific key
   * @param {string|buffer} key
   * @param value
   * @param options
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
        : String(value)

      // insert the value using durability an conflict to emulate sync option and avoid
      // branch statements for insert and update
      return this.$t.insert({ key, value }, { durability, conflict, returnChanges })
        .run(this.$connection)
        .then((results) => {
          let { errors, first_error } = results
          if (errors && first_error) return callback(new Error(first_error))
          return callback()
        })
        .catch((error) => {
          return callback(error.msg ? new Error(error.msg) : error)
        })
    } catch (err) {
      return callback(err)
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
          this.$r.error('NotFound')
        )

    } catch (err) {

    }
  }

  _batch () {

  }

  _chainedBatch () {

  }

  _approximateSize () {

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
  if (!r) throw new Error('no rethinkdb driver was provided')

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