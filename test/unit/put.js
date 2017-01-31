import rethinkdb from 'rethinkdb'
import rethinkdbdash from 'rethinkdbdash'
import rethinkdown from '../../index'

function putRecord (key, value, driver, done) {
  let down = rethinkdown(driver, dbName, dbOptions)(dbTable)
  down.open({ createIfMissing: true }, (error) => {
    if (error) return done(error)
    return down.put(key, value, (error) => {
      if (error) return done(error)
      return down.close((error) => {
        if (error) return done(error)
        return done()
      })
    })
  })
}

function putSingle (key, value, driver, done) {
  let down = rethinkdown(driver, dbName, Object.assign({ singleTable }, dbOptions))(dbTable)
  down.open({ createIfMissing: true }, (error) => {
    if (error) return done(error)
    return down.put(key, value, (error) => {
      if (error) return done(error)
      return down.close((error) => {
        if (error) return done(error)
        return done()
      })
    })
  })
}

export default function testPut () {
  describe('Test put method', () => {
    it('Should put a STRING record into the table using rethinkdbdash', (done) => {
      putRecord('rethinkdbdash', 'putRecord', rethinkdbdash, done)
    })
    it('Should put a STRING record into the table using rethinkdb', (done) => {
      putRecord('rethinkdb', 'putRecord', rethinkdb, done)
    })

    it('Should put a BINARY record into the table using rethinkdbdash', (done) => {
      putRecord('rethinkdbdashBIN', new Buffer('putRecordBinary'), rethinkdbdash, done)
    })
    it('Should put a BINARY record into the table using rethinkdb', (done) => {
      putRecord('rethinkdbBIN', new Buffer('putRecordBinary'), rethinkdb, done)
    })
  })
  describe('Test put method in single mode', () => {
    it('Should put a STRING record into the table using rethinkdbdash', (done) => {
      putSingle('rethinkdbdash', 'putRecord', rethinkdbdash, done)
    })
    it('Should put a STRING record into the table using rethinkdb', (done) => {
      putSingle('rethinkdb', 'putRecord', rethinkdb, done)
    })

    it('Should put a BINARY record into the table using rethinkdbdash', (done) => {
      putSingle('rethinkdbdashBIN', new Buffer('putRecordBinary'), rethinkdbdash, done)
    })
    it('Should put a BINARY record into the table using rethinkdb', (done) => {
      putSingle('rethinkdbBIN', new Buffer('putRecordBinary'), rethinkdb, done)
    })
  })
}