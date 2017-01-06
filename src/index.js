import AbstractLevelDOWN from 'abstract-leveldown'

class RethinkDOWN extends AbstractLevelDOWN {
  constructor (location, r) {
    super(typeof location === 'string' ? location : '')
    this.$r = r
    this.$connection = null
    this.$table = location
  }

  _open (options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }

    try {
      // check for standard rethinkdb driver
      if (typeof this.$r.connect === 'function') {
        return this.$r.connect(options, (err, connection) => {
          if (err) return callback(err)
          this.$connection = connection
          return callback()
        })
      }

      // otherwise use synchronous driver
      this.$r = this.$r(options)
      return callback()
    } catch (err) {
      return callback(err)
    }
  }

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