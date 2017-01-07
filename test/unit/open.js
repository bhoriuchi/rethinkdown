import rethinkdb from 'rethinkdb'
import rethinkdbdash from 'rethinkdbdash'
import rethinkdown from '../../index'

function testOpenClose (driver, done) {
  let down = rethinkdown(driver, dbName, dbOptions)(dbTable)
  down.open({ createIfMissing: true }, (error) => {
    if (error) return done(error)
    return down.close(done)
  })
}

function testErrorIfExists (driver, done) {
  let down = rethinkdown(driver, dbName, dbOptions)(dbTable)
  down.open({ errorIfExists: true }, (error) => {
    if (error) return done()
    return down.close(() => {
      done(new Error('did not throw error when table exists'))
    })
  })
}

export default function testOpen () {
  describe('Test open method', () => {
    // open and close test
    it('Should open the database with rethinkdbdash', (done) => {
      return testOpenClose(rethinkdbdash, done)
    })
    it('Should open the database with rethinkdb', (done) => {
      return testOpenClose(rethinkdb, done)
    })

    // errorIfExists test
    it('Should throw errorIfExists with rethinkdbdash', (done) => {
      return testErrorIfExists(rethinkdbdash, done)
    })
    it('Should throw errorIfExists with rethinkdb', (done) => {
      return testErrorIfExists(rethinkdb, done)
    })
  })
}