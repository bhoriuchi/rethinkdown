'use strict';

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

var util = _interopDefault(require('util'));
var url = _interopDefault(require('url'));
var abstractLeveldown = require('abstract-leveldown');

var _typeof = typeof Symbol === "function" && typeof Symbol.iterator === "symbol" ? function (obj) {
  return typeof obj;
} : function (obj) {
  return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj;
};











var classCallCheck = function (instance, Constructor) {
  if (!(instance instanceof Constructor)) {
    throw new TypeError("Cannot call a class as a function");
  }
};

var createClass = function () {
  function defineProperties(target, props) {
    for (var i = 0; i < props.length; i++) {
      var descriptor = props[i];
      descriptor.enumerable = descriptor.enumerable || false;
      descriptor.configurable = true;
      if ("value" in descriptor) descriptor.writable = true;
      Object.defineProperty(target, descriptor.key, descriptor);
    }
  }

  return function (Constructor, protoProps, staticProps) {
    if (protoProps) defineProperties(Constructor.prototype, protoProps);
    if (staticProps) defineProperties(Constructor, staticProps);
    return Constructor;
  };
}();





var defineProperty = function (obj, key, value) {
  if (key in obj) {
    Object.defineProperty(obj, key, {
      value: value,
      enumerable: true,
      configurable: true,
      writable: true
    });
  } else {
    obj[key] = value;
  }

  return obj;
};



var inherits = function (subClass, superClass) {
  if (typeof superClass !== "function" && superClass !== null) {
    throw new TypeError("Super expression must either be null or a function, not " + typeof superClass);
  }

  subClass.prototype = Object.create(superClass && superClass.prototype, {
    constructor: {
      value: subClass,
      enumerable: false,
      writable: true,
      configurable: true
    }
  });
  if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass;
};











var possibleConstructorReturn = function (self, call) {
  if (!self) {
    throw new ReferenceError("this hasn't been initialised - super() hasn't been called");
  }

  return call && (typeof call === "object" || typeof call === "function") ? call : self;
};

/**
 * RethinkDOWN
 * @description A RethinkDB implementation of the LevelDOWN API
 * @author Branden Horiuchi <bhoriuchi@gmail.com>
 */
var DEFAULT_RETHINKDB_DB = 'test';

// regex
var TABLE_NAME_RX = /\W+/g;

// property values
var PUT_OPERATION = 'put';
var DEL_OPERATION = 'del';
var PK = 'id';
var KEY = 'key';
var VALUE = 'value';

// Error messages
var ERR_DB_DNE = 'Database %s does not exist';
var ERR_INVALID_BATCH_OP = 'Invalid batch operation. Valid operations are "put" and "del"';
var ERR_INVALID_PARAM = 'Invalid parameter %s must be type %s with valid value';
var ERR_INVALID_PRIMARY_KEY = 'Database %s table %s does not have its primary key set ' + 'to "' + PK + '" and cannot be used, please re-create the table with the option ' + '"{ primaryKey: \'' + PK + '\' }" or remove the table and use the "createIfMissing" option' + 'in the open method';
var ERR_NO_RETHINK_DRIVER = 'No RethinkDB driver was provided';
var ERR_NOT_FOUND = 'Key %s not found';
var ERR_TABLE_DNE = 'Table %s does not exist';
var ERR_TABLE_EXISTS = 'TableExists';
var ERR_REQUIRES_CALLBACK = '%s() requires a callback argument';

/**
 * Gets an error object, for rethinkdb errors use the msg field
 * @param error
 * @return {Error}
 */
function DOWNError(error) {
  if (error instanceof Error) {
    return error.msg ? new Error(error.msg) : error;
  } else if ((typeof error === 'undefined' ? 'undefined' : _typeof(error)) === 'object') {
    try {
      return new Error(JSON.stringify(error));
    } catch (err) {
      return new Error(String(error));
    }
  }
  return new Error(String(error));
}

/**
 * Ensures that the value is a buffer or string
 * @param value
 * @param ensureBuffer
 * @return {Buffer}
 */
function asBuffer(value) {
  var ensureBuffer = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : true;

  return ensureBuffer && !Buffer.isBuffer(value) ? new Buffer(value) : Buffer.isBuffer(value) ? value.toString() : value;
}

/**
 * Chained Batch class
 * @extends AbstractChainedBatch
 */

var RethinkChainedBatch = function (_AbstractChainedBatch) {
  inherits(RethinkChainedBatch, _AbstractChainedBatch);

  /**
   * Creates a new RethinkChainedBatch
   * @param {object} db - rethinkdown instance
   */
  function RethinkChainedBatch(db) {
    classCallCheck(this, RethinkChainedBatch);
    return possibleConstructorReturn(this, (RethinkChainedBatch.__proto__ || Object.getPrototypeOf(RethinkChainedBatch)).call(this, db));
  }

  return RethinkChainedBatch;
}(abstractLeveldown.AbstractChainedBatch);

/**
 * Rethink Iterator
 * @extends AbstractIterator
 */


var RethinkIterator = function (_AbstractIterator) {
  inherits(RethinkIterator, _AbstractIterator);

  /**
   * Creates a new Iterator
   * @param {object} db - RethinkDOWN instance
   * @param {object} [options]
   */
  function RethinkIterator(db) {
    var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
    classCallCheck(this, RethinkIterator);

    var _this2 = possibleConstructorReturn(this, (RethinkIterator.__proto__ || Object.getPrototypeOf(RethinkIterator)).call(this, db));

    var r = db.$r;
    var query = db.$t;

    var gt = options.gt,
        gte = options.gte,
        lt = options.lt,
        lte = options.lte,
        start = options.start,
        end = options.end,
        reverse = options.reverse,
        keys = options.keys,
        values = options.values,
        limit = options.limit,
        keyAsBuffer = options.keyAsBuffer,
        valueAsBuffer = options.valueAsBuffer;

    _this2._keyAsBuffer = keyAsBuffer !== false;
    _this2._valueAsBuffer = valueAsBuffer !== false;

    // logic ported from mongodown - https://github.com/watson/mongodown
    if (reverse) {
      if (start) query = query.filter(r.row(PK).le(start));
      if (end) query = query.filter(r.row(PK).ge(end));
      if (gt) query = query.filter(r.row(PK).lt(gt));
      if (gte) query = query.filter(r.row(PK).le(gte));
      if (lt) query = query.filter(r.row(PK).gt(lt));
      if (lte) query = query.filter(r.row(PK).ge(lte));
    } else {
      if (start) query = query.filter(r.row(PK).ge(start));
      if (end) query = query.filter(r.row(PK).le(end));
      if (gt) query = query.filter(r.row(PK).gt(gt));
      if (gte) query = query.filter(r.row(PK).ge(gte));
      if (lt) query = query.filter(r.row(PK).lt(lt));
      if (lte) query = query.filter(r.row(PK).le(lte));
    }

    // sort the results by key depending on reverse value
    query = query.orderBy(reverse ? r.desc(PK) : r.asc(PK));

    // set limit
    query = typeof limit === 'number' && limit >= 0 ? query.limit(limit) : query;

    // run query
    _this2._query = db.$sync ? query.run({ cursor: true }) : query.run(db.$connection);
    return _this2;
  }

  /**
   * Gets the next key in the iterator results
   * @callback callback
   * @return {*}
   * @private
   */


  createClass(RethinkIterator, [{
    key: '_next',
    value: function _next(callback) {
      var _this3 = this;

      return this._query.then(function (cursor) {
        try {
          cursor.next(function (error, row) {
            if (error) {
              return error.name === 'ReqlDriverError' && error.message === 'No more rows in the cursor.' ? callback() : callback(DOWNError(error));
            }

            var key = row[PK],
                value = row.value;

            key = asBuffer(key, _this3._keyAsBuffer);
            value = asBuffer(value, _this3._valueAsBuffer);
            return callback(null, key, value);
          });
        } catch (error) {
          return callback(DOWNError(error));
        }
      });
    }

    /**
     * Destroys the iterator and closes the cursor
     * @callback callback
     * @private
     */

  }, {
    key: '_end',
    value: function _end(callback) {
      try {
        return this._query.then(function (cursor) {
          return cursor.close(function (error) {
            return error ? callback(DOWNError(error)) : callback();
          });
        }, function (error) {
          return callback(DOWNError(error));
        });
      } catch (error) {
        return callback(DOWNError(error));
      }
    }

    // not implemented in abstract?

  }, {
    key: 'seek',
    value: function seek(key) {
      throw DOWNError('seek is not implemented');
    }
  }]);
  return RethinkIterator;
}(abstractLeveldown.AbstractIterator);

/**
 * RethinkDOWN class
 * @extends AbstractLevelDOWN
 */


var RethinkDOWN = function (_AbstractLevelDOWN) {
  inherits(RethinkDOWN, _AbstractLevelDOWN);

  /**
   * Initializes the connector and parses the location/connection string
   * @param {string} location - connection string in the form rethinkdb://[<user>:<password>@]<host>[:<port>][/<db>]/<table>[?timeout=<timeout>]
   * @param {object} r - rethinkdb driver
   */
  function RethinkDOWN(location, r, db, options) {
    classCallCheck(this, RethinkDOWN);

    // validate that the location is a string and replace any invalid characters with _
    var _this4 = possibleConstructorReturn(this, (RethinkDOWN.__proto__ || Object.getPrototypeOf(RethinkDOWN)).call(this, location));

    if (typeof location !== 'string') throw DOWNError(util.format(ERR_INVALID_PARAM, 'location', 'String'));

    // store rethinkdb driver and initialize connection
    _this4.$r = r;
    _this4.$connection = null;
    _this4.$t = null;
    _this4.$db = db;
    _this4.$table = location.replace(TABLE_NAME_RX, '_');
    _this4.$sync = false;
    _this4.$options = options;
    return _this4;
  }

  /**
   * opens a database connection and optionally creates the database and/or table
   * @param {object} [options]
   * @callback callback
   * @returns {*}
   * @private
   */


  createClass(RethinkDOWN, [{
    key: '_open',
    value: function _open(options, callback) {
      var _this5 = this;

      try {
        var _ret = function () {
          // support some of the open options
          var createIfMissing = options.createIfMissing,
              errorIfExists = options.errorIfExists;

          createIfMissing = typeof createIfMissing === 'boolean' ? createIfMissing : true;
          errorIfExists = typeof errorIfExists === 'boolean' ? errorIfExists : false;

          // processes open options to create or throw error
          var processOptions = function processOptions(r) {
            return r.dbList().contains(_this5.$db).branch(r.db(_this5.$db).tableList().contains(_this5.$table).branch(r.expr(errorIfExists).branch(r.error(ERR_TABLE_EXISTS), r.db(_this5.$db).table(_this5.$table).config()('primary_key').eq(PK).branch(true, r.error(util.format(ERR_INVALID_PRIMARY_KEY, _this5.$db, _this5.$table)))), r.expr(createIfMissing).branch(r.db(_this5.$db).tableCreate(_this5.$table, { primaryKey: PK }), r.error(util.format(ERR_TABLE_DNE, _this5.$table)))), r.expr(createIfMissing).branch(r.dbCreate(_this5.$db).do(function () {
              return r.db(_this5.$db).tableCreate(_this5.$table, { primaryKey: PK });
            }), r.error(util.format(ERR_DB_DNE, _this5.$db)))).run(_this5.$connection, function (error) {
              if (error) return callback(DOWNError(error));
              _this5.$t = r.db(_this5.$db).table(_this5.$table);
              return callback();
            });
          };

          // check for standard rethinkdb driver
          if (typeof _this5.$r.connect === 'function') {
            _this5.$sync = false;
            return {
              v: _this5.$r.connect(_this5.$options, function (error, connection) {
                if (error) return callback(DOWNError(error));
                _this5.$connection = connection;
                return processOptions(_this5.$r);
              })
            };
          }

          // otherwise use synchronous driver (rethinkdbdash)
          _this5.$r = _this5.$r(_this5.$options);
          _this5.$sync = true;
          _this5.$connection = {};
          return {
            v: processOptions(_this5.$r)
          };
        }();

        if ((typeof _ret === 'undefined' ? 'undefined' : _typeof(_ret)) === "object") return _ret.v;
      } catch (error) {
        return callback(DOWNError(error));
      }
    }

    /**
     * closes a database connection or cleans up the pool
     * @callback callback
     * @returns {*}
     * @private
     */

  }, {
    key: '_close',
    value: function _close(callback) {
      try {
        if (this.$connection && typeof this.$connection.close === 'function') {
          return this.$connection.close(callback);
        } else if (typeof this.$r.getPoolMaster === 'function') {
          this.$r.getPoolMaster().drain();
          return callback();
        }
      } catch (error) {
        return callback(DOWNError(error));
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

  }, {
    key: '_get',
    value: function _get(key, options, callback) {
      var _this6 = this;

      try {
        var _ret2 = function () {
          var r = _this6.$r;
          var table = _this6.$t;
          var asBuffer = options.asBuffer;

          // get the key

          return {
            v: table.get(key).default(null).do(function (result) {
              return result.eq(null).branch(r.error(util.format(ERR_NOT_FOUND, key)), result(VALUE).default(null).do(function (value) {
                return r.expr([null, '']).contains(value).or(value.typeOf().eq('ARRAY').and(value.count().eq(0))).branch('', value);
              }).do(function (value) {
                return r.expr(asBuffer).branch(value.coerceTo('BINARY'), value.coerceTo('STRING'));
              }));
            }).run(_this6.$connection, function (error, value) {
              return error ? callback(DOWNError(error)) : callback(null, value);
            })
          };
        }();

        if ((typeof _ret2 === 'undefined' ? 'undefined' : _typeof(_ret2)) === "object") return _ret2.v;
      } catch (error) {
        return callback(DOWNError(error));
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

  }, {
    key: '_put',
    value: function _put(key, value, options, callback) {
      try {
        var _$t$insert;

        var sync = options.sync;

        var conflict = 'update';
        var returnChanges = true;
        var durability = sync === true ? 'hard' : 'soft';

        // insert the value using durability an conflict to emulate sync option and avoid
        // branch statements for insert and update
        return this.$t.insert((_$t$insert = {}, defineProperty(_$t$insert, PK, key), defineProperty(_$t$insert, 'value', value), _$t$insert), { conflict: conflict, durability: durability, returnChanges: returnChanges }).pluck('errors', 'first_error').run(this.$connection, function (error, results) {
          if (error) return callback(DOWNError(error));
          var errors = results.errors,
              first_error = results.first_error;

          if (errors && first_error) return callback(DOWNError(first_error));
          return callback();
        });
      } catch (error) {
        return callback(DOWNError(error));
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

  }, {
    key: '_del',
    value: function _del(key, options, callback) {
      try {
        var sync = options.sync;

        var returnChanges = true;
        var durability = sync === true ? 'hard' : 'soft';

        // delete the record
        return this.$t.get(key).eq(null).branch(this.$r.error(util.format(ERR_NOT_FOUND, key)), this.$t.get(key).delete({ durability: durability, returnChanges: returnChanges })).pluck('errors', 'first_error').run(this.$connection, function (error, results) {
          if (error) return callback(DOWNError(error));
          var errors = results.errors,
              first_error = results.first_error;

          if (errors && first_error) return callback(DOWNError(first_error));
          return callback();
        });
      } catch (error) {
        return callback(DOWNError(error));
      }
    }

    /**
     * Performs batch operations of put and/or delete
     * @param {array} operations
     * @param {object} [options]
     * @callback callback
     * @private
     */

  }, {
    key: '_batch',
    value: function _batch(operations, options, callback) {
      var _this7 = this;

      try {
        var _ret3 = function () {
          var ops = [];
          var sync = options.sync;

          var returnChanges = true;
          var conflict = 'update';
          var durability = sync === true ? 'hard' : 'soft';

          // validate the operations list
          var _iteratorNormalCompletion = true;
          var _didIteratorError = false;
          var _iteratorError = undefined;

          try {
            for (var _iterator2 = operations[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator2.next()).done); _iteratorNormalCompletion = true) {
              var operation = _step.value;
              var type = operation.type,
                  key = operation.key,
                  value = operation.value;

              // determine the operation type and create an operation object

              switch (type) {
                case PUT_OPERATION:
                  // coerce the value into a valid value
                  value = _this7._serializeValue(value);
                  ops.push({ type: PUT_OPERATION, key: key, value: value });
                  break;

                case DEL_OPERATION:
                  ops.push({ type: DEL_OPERATION, key: key });
                  break;

                default:
                  return {
                    v: callback(DOWNError(ERR_INVALID_BATCH_OP))
                  };
              }
            }

            // perform operations
          } catch (err) {
            _didIteratorError = true;
            _iteratorError = err;
          } finally {
            try {
              if (!_iteratorNormalCompletion && _iterator2.return) {
                _iterator2.return();
              }
            } finally {
              if (_didIteratorError) {
                throw _iteratorError;
              }
            }
          }

          return {
            v: _this7.$r.expr(ops).forEach(function (op) {
              var _this7$$t$insert;

              return op('type').eq(PUT_OPERATION).branch(_this7.$t.insert((_this7$$t$insert = {}, defineProperty(_this7$$t$insert, PK, op(KEY)), defineProperty(_this7$$t$insert, 'value', op(VALUE)), _this7$$t$insert), { conflict: conflict, durability: durability, returnChanges: returnChanges }), _this7.$t.get(op(KEY)).eq(null).branch(_this7.$r.error(util.format(ERR_NOT_FOUND, op(KEY))), _this7.$t.get(op(KEY)).delete({ durability: durability, returnChanges: returnChanges })));
            }).pluck('errors', 'first_error').run(_this7.$connection, function (error, results) {
              if (error) return callback(DOWNError(error));
              var errors = results.errors,
                  first_error = results.first_error;

              if (errors && first_error) return callback(DOWNError(first_error));
              return callback();
            })
          };
        }();

        if ((typeof _ret3 === 'undefined' ? 'undefined' : _typeof(_ret3)) === "object") return _ret3.v;
      } catch (error) {
        return callback(DOWNError(error));
      }
    }

    /**
     * Returns a new chained batch
     * @return {RethinkChainedBatch}
     * @private
     */

  }, {
    key: '_chainedBatch',
    value: function _chainedBatch() {
      return new RethinkChainedBatch(this);
    }

    /**
     * Gets count of records, doesnt really apply/work with rethinkdb
     * @param {string|buffer} start
     * @param {string|buffer} end
     * @callback callback
     * @return {number}
     * @private
     */

  }, {
    key: '_approximateSize',
    value: function _approximateSize(start, end, callback) {
      if (typeof callback !== 'function') {
        throw DOWNError(util.format(ERR_REQUIRES_CALLBACK, 'approximateSize'));
      } else if (typeof start !== 'string' && !(start instanceof Buffer)) {
        return callback(DOWNError(util.format(ERR_INVALID_PARAM, 'start', 'string or Buffer')));
      } else if (typeof end !== 'string' && !(end instanceof Buffer)) {
        return callback(DOWNError(util.format(ERR_INVALID_PARAM, 'end', 'string or Buffer')));
      }

      try {
        return this.$t.filter(function (record) {
          return record.ge(start).and(record.le(end));
        }).count().run(this.$connection, function (error, size) {
          if (error) return callback(DOWNError(error));
          return callback(null, size);
        });
      } catch (error) {
        return callback(DOWNError(error));
      }
    }

    /**
     * Returns a new iterator
     * @param options
     * @return {RethinkIterator}
     * @private
     */

  }, {
    key: '_iterator',
    value: function _iterator(options) {
      return new RethinkIterator(this, options);
    }
  }]);
  return RethinkDOWN;
}(abstractLeveldown.AbstractLevelDOWN);

/**
 * RethinkDOWN
 * @param {object} r - RethinkDB driver
 * @return {DOWN}
 */


var index = function (r, db) {
  var options = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {};

  if ((typeof db === 'undefined' ? 'undefined' : _typeof(db)) === 'object') {
    options = db;
    db = DEFAULT_RETHINKDB_DB;
  }

  // check for driver and set db
  if (!r) throw DOWNError(ERR_NO_RETHINK_DRIVER);
  db = db || DEFAULT_RETHINKDB_DB;

  /**
   *
   * @param {string} location - connection string in the form rethinkdb://[<user>:<password>@]<host>[:<port>][/<db>]/<table>[?timeout=<timeout>]
   * @return {RethinkDOWN}
   * @constructor
   */
  var DOWN = function DOWN(location) {
    return new RethinkDOWN(location, r, db, options);
  };

  /**
   * Connects to the database and returns the cursor, connection, and parsed location
   * @param {string} location - connection string
   * @callback callback
   * @return {*}
   */
  var connect = function connect(callback) {
    if (typeof callback !== 'function') throw DOWNError(util.format(ERR_REQUIRES_CALLBACK, 'connect'));

    try {
      // async connection
      if (typeof r.connect === 'function') {
        return r.connect(options, function (error, connection) {
          if (error) return callback(DOWNError(error));
          return callback(null, r, connection);
        });
      }

      // sync connection
      r = r(options);
      return callback(null, r, {});
    } catch (error) {
      return callback(DOWNError(error));
    }
  };

  /**
   * Destroys the table specified by the location
   * @param {string} location - connection string
   * @callback callback
   * @return {*}
   */
  DOWN.destroy = function (location, callback) {
    if (typeof callback !== 'function') throw DOWNError(util.format(ERR_REQUIRES_CALLBACK, 'destroy'));
    if (typeof location !== 'string') throw DOWNError(util.format(ERR_INVALID_PARAM, 'db', 'String'));
    try {
      var _ret4 = function () {
        var table = location.replace(TABLE_NAME_RX, '_');
        return {
          v: connect(function (error, cursor, connection) {
            if (error) return callback(DOWNError(error));
            return cursor.db(db).tableDrop(table).run(connection, function (error) {
              if (error) callback(DOWNError(error));
              return callback();
            });
          })
        };
      }();

      if ((typeof _ret4 === 'undefined' ? 'undefined' : _typeof(_ret4)) === "object") return _ret4.v;
    } catch (error) {
      return callback(DOWNError(error));
    }
  };

  DOWN.repair = function (location, callback) {
    if (typeof callback !== 'function') throw DOWNError(util.format(ERR_REQUIRES_CALLBACK, 'repair'));
    return callback(DOWNError('repair not implemented'));
  };

  // return the creation method
  return DOWN;
};

module.exports = index;
