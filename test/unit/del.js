import rethinkdb from 'rethinkdb'
import rethinkdbdash from 'rethinkdbdash'
import rethinkdown from '../../index'

function delRecord (key, driver, done) {
  let down = rethinkdown(driver, dbName, dbOptions)(dbTable)
  down.open({ createIfMissing: true }, (error) => {
    if (error) return done(error)
    return down.del(key, (error) => {
      if (error) return done(error)
      return down.close((error) => {
        if (error) return done(error)
        return done()
      })
    })
  })
}

function delSingle (key, driver, done) {
  let down = rethinkdown(driver, dbName, Object.assign({ singleTable }, dbOptions))(dbTable)
  down.open({ createIfMissing: true }, (error) => {
    if (error) return done(error)
    return down.del(key, (error) => {
      if (error) return done(error)
      return down.close((error) => {
        if (error) return done(error)
        return done()
      })
    })
  })
}

export default function testDel () {
  describe('Test del method', () => {
    it('Should delete a key using rethinkdbdash', (done) => {
      delRecord('rethinkdbdashBIN', rethinkdbdash, done)
    })
    it('Should delete a key using rethinkdb', (done) => {
      delRecord('rethinkdbBIN', rethinkdb, done)
    })
  })

  describe('Test del method in single mode', () => {
    it('Should delete a key using rethinkdbdash', (done) => {
      delSingle('rethinkdbdashBIN', rethinkdbdash, done)
    })
    it('Should delete a key using rethinkdb', (done) => {
      delSingle('rethinkdbBIN', rethinkdb, done)
    })
  })
}