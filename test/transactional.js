'use strict'

const assert = require('assert')
const pg = require('../')

const config = 'postgres://hx@localhost/hx'

describe('transactional', () => {
  before(() => {
    return pg.Pool(config).query('CREATE TABLE account (id int, amount int)')
  })

  after(() => {
    return pg.Pool(config).query('DROP TABLE account')
  })

  beforeEach(() => {
    let pool = pg.Pool(config)
    return pool.query('TRUNCATE account')
    .then(() => {
      return pool.query('INSERT INTO account (id, amount) VALUES (1, 100), (2, 100)')
    })
  })

  describe('pg.Pool', () => {
    it('handles a simple query', () => {
      return pg.Pool(config)
      .transactional(tx => {
        return tx.query('SELECT 1 AS count')
      })
      .then((result) => {
        assert.equal(result.rowCount, 1)
        assert.equal(result.rows[0].count, 1)
      })
    })

    it('runs two updates in a transaction', () => {
      let pool = pg.Pool(config)
      return pool
      .transactional(tx => {
        let inc = tx.query('UPDATE account SET amount = amount + 10 WHERE id = 1')
        let dec = tx.query('UPDATE account SET amount = amount - 10 WHERE id = 2')
        return Promise.all([inc, dec])
      })
      .then((result) => {
        return pool.query('SELECT id, amount FROM account ORDER BY id ASC')
      })
      .then((result) => {
        assert.equal(result.rowCount, 2)
        assert.equal(result.rows[0].amount, 110)
        assert.equal(result.rows[1].amount, 90)
      })
    })

    it('properly rolls back on exception', () => {
      let pool = pg.Pool(config)
      return pool
      .transactional(tx => {
        let inc = tx.query('UPDATE account SET amount = amount + 10 WHERE id = 1')
        let dec = tx.query('UPDATE account SET amount = amount - 10 WHERE id = 3')
        return Promise.all([inc, dec])
        .then(res => {
          for (let r of res) {
            if (res.rowCount != 1) {
              throw new Error('One or more accounts was not updated')
            }
          }
        })
      })
      .catch((err) => {
        return pool.query('SELECT id, amount FROM account ORDER BY id ASC')
      })
      .then((result) => {
        // Make sure the accounts did not change
        assert.equal(result.rowCount, 2)
        assert.equal(result.rows[0].amount, 100)
        assert.equal(result.rows[1].amount, 100)
      })
    })

    it('properly rolls back on rollback', () => {
      let pool = pg.Pool(config)
      return pool
      .transactional(tx => {
        let inc = tx.query('UPDATE account SET amount = amount + 10 WHERE id = 1')
        let dec = tx.query('UPDATE account SET amount = amount - 10 WHERE id = 3')
        return Promise.all([inc, dec])
        .then(res => {
          for (let r of res) {
            if (res.rowCount != 1) {
              return tx.rollback()
            }
          }
        })
      })
      .then((result) => {
        return pool.query('SELECT id, amount FROM account ORDER BY id ASC')
      })
      .then((result) => {
        // Make sure the accounts did not change
        assert.equal(result.rowCount, 2)
        assert.equal(result.rows[0].amount, 100)
        assert.equal(result.rows[1].amount, 100)
      })
    })

    it('uses parameters', () => {
      let pool = pg.Pool(config)
      return pool
      .transactional(tx => {
        let amount = 10
        let account1 = 1
        let account2 = 2
        let inc = tx.query('UPDATE account SET amount = amount + $1 WHERE id = $2', [amount, account1])
        let dec = tx.query('UPDATE account SET amount = amount - $1 WHERE id = $2', [amount, account2])
        return Promise.all([inc, dec])
      })
      .then((result) => {
        return pool.query('SELECT id, amount FROM account ORDER BY id ASC')
      })
      .then((result) => {
        assert.equal(result.rowCount, 2)
        assert.equal(result.rows[0].amount, 110)
        assert.equal(result.rows[1].amount, 90)
      })
    })
  })

  describe('pg.Client', () => {
    let client = null

    beforeEach(() => {
      client = pg.Client(config)
    })
    afterEach(() => {
      client.end()
      client = null
    })

    it('handles a simple query', () => {
      return client
      .transactional(tx => {
        return tx.query('SELECT 1 AS count')
      })
      .then((result) => {
        assert.equal(result.rowCount, 1)
        assert.equal(result.rows[0].count, 1)
      })
      .then(() => client.end())
    })

    it('runs two updates in a transaction', () => {
      return client
      .transactional(tx => {
        let inc = tx.query('UPDATE account SET amount = amount + 10 WHERE id = 1')
        let dec = tx.query('UPDATE account SET amount = amount - 10 WHERE id = 2')
        return Promise.all([inc, dec])
      })
      .then((result) => {
        return client.query('SELECT id, amount FROM account ORDER BY id ASC')
      })
      .then((result) => {
        assert.equal(result.rowCount, 2)
        assert.equal(result.rows[0].amount, 110)
        assert.equal(result.rows[1].amount, 90)
      })
      .then(() => client.end())
    })

    it('properly rolls back on exception', () => {
      return client
      .transactional(tx => {
        let inc = tx.query('UPDATE account SET amount = amount + 10 WHERE id = 1')
        let dec = tx.query('UPDATE account SET amount = amount - 10 WHERE id = 3')
        return Promise.all([inc, dec])
        .then(res => {
          for (let r of res) {
            if (res.rowCount != 1) {
              throw new Error('One or more accounts was not updated')
            }
          }
        })
      })
      .catch((err) => {
        return client.query('SELECT id, amount FROM account ORDER BY id ASC')
      })
      .then((result) => {
        // Make sure the accounts did not change
        assert.equal(result.rowCount, 2)
        assert.equal(result.rows[0].amount, 100)
        assert.equal(result.rows[1].amount, 100)
      })
    })

    it('properly rolls back on rollback', () => {
      return client
      .transactional(tx => {
        let inc = tx.query('UPDATE account SET amount = amount + 10 WHERE id = 1')
        let dec = tx.query('UPDATE account SET amount = amount - 10 WHERE id = 3')
        return Promise.all([inc, dec])
        .then(res => {
          for (let r of res) {
            if (res.rowCount != 1) {
              return tx.rollback()
            }
          }
        })
      })
      .then((result) => {
        return client.query('SELECT id, amount FROM account ORDER BY id ASC')
      })
      .then((result) => {
        // Make sure the accounts did not change
        assert.equal(result.rowCount, 2)
        assert.equal(result.rows[0].amount, 100)
        assert.equal(result.rows[1].amount, 100)
      })
    })

    it('uses parameters', () => {
      return client
      .transactional(tx => {
        let amount = 10
        let account1 = 1
        let account2 = 2
        let inc = tx.query('UPDATE account SET amount = amount + $1 WHERE id = $2', [amount, account1])
        let dec = tx.query('UPDATE account SET amount = amount - $1 WHERE id = $2', [amount, account2])
        return Promise.all([inc, dec])
      })
      .then((result) => {
        return client.query('SELECT id, amount FROM account ORDER BY id ASC')
      })
      .then((result) => {
        assert.equal(result.rowCount, 2)
        assert.equal(result.rows[0].amount, 110)
        assert.equal(result.rows[1].amount, 90)
      })
    })
  })

  describe('race conditions', () => {
    function delay(timeout) {
      return new Promise(resolve => {
        setTimeout(() => {
          resolve()
        }, timeout)
      })
    }

    it('a', () => {
      const pool = pg.Pool(config)

      // Test a query that gets started before and finishes after an error doesn't wreak havoc
      return pool
        .transactional(tx => {
          const q1 = tx.query('SELECT pg_sleep(2); UPDATE account SET amount = 100000 WHERE id = 1')
          const q2 = delay(1000).then(() => { throw new Error("ERROR") })
          return Promise.all([q1])
        })
        .catch(() => delay(1200))
        .then(() => {
          return pool.query('SELECT id, amount FROM account ORDER BY id ASC')
        })
        .then((result) => {
          // Make sure the accounts did not change
          assert.equal(result.rowCount, 2)
          assert.equal(result.rows[0].amount, 100)
          assert.equal(result.rows[1].amount, 100)
        })
    })

    it('b', () => {
      const pool = pg.Pool(config)

      // Test that trying to run a query AFTER another query has already rejected won't wreak havoc
      return pool
        .transactional(tx => {
          const q1 = delay(1000).then(() => tx.query('UPDATE account SET amount = 100000 WHERE id = 1'))
          const q2 = Promise.reject(new Error("ERROR"))
          return Promise.all([q1, q2])
        })
        .catch(() => delay(1200))
        .then(() => {
          return pool.query('SELECT id, amount FROM account ORDER BY id ASC')
        })
        .then((result) => {
          // Make sure the accounts did not change
          assert.equal(result.rowCount, 2)
          assert.equal(result.rows[0].amount, 100)
          assert.equal(result.rows[1].amount, 100)
        })
    })

    it('c', () => {
      const pool = pg.Pool(config)

      // Test that an error reported from the server on the connection doesn't sink us
      return pool
        .transactional(tx => {
          const q1 = tx.query('SELECT pg_sleep(1); UPDATE account SET amount = 100000 WHERE id = 1')
          const q2 = delay(1500).then(() => tx.query('SELECT * FROM nonexistanttable'))
          return Promise.all([q1, q2])
        })
        .catch(() => delay(2200))
        .then(() => {
          return pool.query('SELECT id, amount FROM account ORDER BY id ASC')
        })
        .then((result) => {
          // Make sure the accounts did not change
          assert.equal(result.rowCount, 2)
          assert.equal(result.rows[0].amount, 100)
          assert.equal(result.rows[1].amount, 100)
        })
    })
  })
})
