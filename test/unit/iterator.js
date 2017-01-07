import rethinkdb from 'rethinkdb'
import rethinkdbdash from 'rethinkdbdash'
import rethinkdown from '../../index'

function iteratorTest (options, nexts, driver, done) {
  let nextCount = 0
  let down = rethinkdown(driver)(rethinkLocation)

  let cleanup = (error) => {
    return down.close(() => {
      return done(error)
    })
  }

  down.open({ createIfMissing: true }, (error) => {
    if (error) return done(error)
    let i = down.iterator(options)

    let cb = (error, value) => {
      nextCount++
      console.log(value)
      if (error) return cleanup(error)
      if (value === undefined) return cleanup()
      if (nextCount >= nexts) return cleanup()
      i.next(cb)
    }

    return i.next(cb)
  })
}

export default function testIterator () {
  describe('Test iterator', () => {
    it('Should iterate through a query rethinkdbdash', (done) => {
      iteratorTest({}, 10, rethinkdbdash, done)
    })
    it('Should iterate through a query rethinkdb', (done) => {
      iteratorTest({}, 10, rethinkdb, done)
    })
  })
}