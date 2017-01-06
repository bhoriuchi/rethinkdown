import url from 'url'
import AbstractLevelDOWN from 'abstract-leveldown'

const DEFAULT_RETHINKDB_PORT = 28015
const DEFAULT_RETHINKDB_DB = 'test'
const TIMEOUT_RX = /(.*)(timeout=)(\d+)(.*)/i

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

    // parse the connection string for connection options
    let { auth, hostname, port, query, pathname } = url.parse(location)
    this.$connectOptions = {}

    // add host
    if (!hostname) throw new Error('no hostname specified in connection string')
    else this.$connectOptions.host = hostname

    // add port
    this.$connectOptions = port
      ? port
      : DEFAULT_RETHINKDB_PORT

    // add db and table
    let [ , db, table ] = (pathname ? pathname : '').split('/')
    if (!db) throw new Error('no database or table specified in connection string')
    if (!table) {
      table = db
      db = DEFAULT_RETHINKDB_DB
    }
    this.$connectOptions.db = db
    this.$table = table

    // add authentication
    let [ user, password ] = (auth ? auth : '').split(':')
    if (user) this.$connectOptions.user = user
    if (password) this.$connectOptions.password = password

    // add timeout
    if (query && query.match(TIMEOUT_RX)) {
      this.$connectOptions.timeout = Number(query.replace(TIMEOUT_RX, '$3'))
    }
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
                    r.error(`table ${this.$table} exists`),
                    true
                  ),
                r.expr(createIfMissing)
                  .branch(
                    r.db(db).tableCreate(this.$table),
                    r.error(`table ${this.table} does not exist`)
                  )
              ),
            r.expr(createIfMissing)
              .branch(
                r.dbCreate(db).do(() => r.db(db).tableCreate(this.table)),
                r.error(`database ${db} does not exist`)
              )
          )
          .run(this.$connection)
          .then(() => {
            return callback()
          })
          .catch(callback)
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

  _get (key, options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }

  }

  _put (key, value, options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
  }
}

export default function (r) {
  return function (location) {
    return new RethinkDOWN(location, r)
  }
}