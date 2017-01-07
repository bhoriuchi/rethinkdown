import rethinkdb from 'rethinkdb'
import rethinkdbdash from 'rethinkdbdash'
import rethinkdown from '../../index'

function batchRecord (ops, driver, done) {
  let down = rethinkdown(driver, dbName, dbOptions)(dbTable)
  down.open({ createIfMissing: true }, (error) => {
    if (error) return done(error)
    return down.batch(ops, (error) => {
      if (error) return done(error)
      return down.close((error) => {
        if (error) return done(error)
        return done()
      })
    })
  })
}

const ops1dash = [
  { type: 'put', key: 'rethinkdbdashbatch1', value: 'dashb1value' },
  { type: 'put', key: 'rethinkdbdashbatch2', value: 'dashb2value' },
  { type: 'put', key: 'rethinkdbdashbatch3', value: 'dashb3value' }
]

const ops1db = [
  { type: 'put', key: 'rethinkdbbatch1', value: 'dbb1value' },
  { type: 'put', key: 'rethinkdbbatch2', value: 'dbb2value' },
  { type: 'put', key: 'rethinkdbbatch3', value: 'dbb3value' }
]

const ops2dash = [
  { type: 'put', key: 'rethinkdbdashbatch4', value: 'dashb4value' },
  { type: 'del', key: 'rethinkdbdashbatch1' },
  { type: 'del', key: 'rethinkdbdashbatch2' }
]

const ops2db = [
  { type: 'put', key: 'rethinkdbbatch4', value: 'dbb4value' },
  { type: 'del', key: 'rethinkdbbatch1' },
  { type: 'del', key: 'rethinkdbbatch2' }
]

export default function testBatch () {
  describe('Test batch method', () => {
    it('Should batch add using rethinkdbdash', (done) => {
      batchRecord(ops1dash, rethinkdbdash, done)
    })
    it('Should batch add using rethinkdb', (done) => {
      batchRecord(ops1db, rethinkdb, done)
    })

    it('Should batch add and del using rethinkdbdash', (done) => {
      batchRecord(ops2dash, rethinkdbdash, done)
    })
    it('Should batch add and del using rethinkdb', (done) => {
      batchRecord(ops2db, rethinkdb, done)
    })
  })
}