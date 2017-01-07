import chai from 'chai'
import unitTests from './unit/index'
global.chai = chai
global.expect = chai.expect
global.rethinkLocation = 'rethinkdb://localhost/test/rethinkdown?silent=true'

// run tests
describe('RethinkDOWN Tests', () => {
  unitTests()
})
