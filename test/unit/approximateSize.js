import rethinkdb from 'rethinkdb'
import rethinkdbdash from 'rethinkdbdash'
import rethinkdown from '../../index'

function approx (start, end, driver, done) {
  let down = rethinkdown(driver, dbName, dbOptions)(dbTable)
  down.open({ createIfMissing: true }, (error) => {
    if (error) return done(error)
    return down.approximateSize(start, end, (error, size) => {
      if (error) return done(error)
      // console.log(size)
      return down.close((error) => {
        if (error) return done(error)
        return done()
      })
    })
  })
}

export default function testApprox () {
  describe('Test approximateSize method', () => {
    it('Should approximate using rethinkdbdash', (done) => {
      approx('r', 'rethinkdbdashbatch4', rethinkdbdash, done)
    })
    it('Should approximate using rethinkdb', (done) => {
      approx('r', 'rethinkdbbatch', rethinkdb, done)
    })
  })
}