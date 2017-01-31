import chai from 'chai'
import unitTests from './unit/index'
global.chai = chai
global.expect = chai.expect
global.dbName = 'test'
global.dbOptions = { silent: true }
global.dbTable = 'rethinkdown'
global.singleTable = 'rethinkdown_single'

// run tests
describe('RethinkDOWN Tests', () => {
  unitTests()
})
