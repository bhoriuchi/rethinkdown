import rethinkdb from 'rethinkdb'
import rethinkdbdash from 'rethinkdbdash'
import rethinkdown from '../../index'

function getRecord (key, val, asBuffer, driver, done) {
  let down = rethinkdown(driver, dbName, dbOptions)(dbTable)
  down.open({ createIfMissing: true }, (error) => {
    if (error) return done(error)
    return down.get(key, { asBuffer }, (error, value) => {
      if (error) return done(error)

      if (asBuffer) {
        if (!Buffer.isBuffer(value)) return done(new Error('value is not a buffer'))
        expect(value.toString()).to.equal(val)
      } else {
        expect(value).to.equal(val)
      }

      return down.close((error) => {
        if (error) return done(error)
        return done()
      })
    })
  })
}

export default function testGet () {
  describe('Test get method', () => {
    it('Should get a STRING using rethinkdbdash', (done) => {
      getRecord('rethinkdbdash', 'putRecord', false, rethinkdbdash, done)
    })
    it('Should get a STRING using rethinkdb', (done) => {
      getRecord('rethinkdb', 'putRecord', false, rethinkdb, done)
    })

    it('Should get a BINARY using rethinkdbdash', (done) => {
      getRecord('rethinkdbdash', 'putRecord', true, rethinkdbdash, done)
    })
    it('Should get a BINARY using rethinkdb', (done) => {
      getRecord('rethinkdb', 'putRecord', true, rethinkdb, done)
    })

    it('Should get a BINARY as STRING using rethinkdbdash', (done) => {
      getRecord('rethinkdbdashBIN', 'putRecordBinary', false, rethinkdbdash, done)
    })
    it('Should get a BINARY as STRING using rethinkdb', (done) => {
      getRecord('rethinkdbBIN', 'putRecordBinary', false, rethinkdb, done)
    })

  })
}