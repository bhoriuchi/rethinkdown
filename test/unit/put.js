import rethinkdb from 'rethinkdb'
import rethinkdbdash from 'rethinkdbdash'
import rethinkdown from '../../index'

function putRecord (key, value, driver, done) {
  let down = rethinkdown(driver)(rethinkLocation)
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
}