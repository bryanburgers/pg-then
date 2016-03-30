
'use strict'

const PassThrough = require('stream').PassThrough
const QueryStream = require('pg-query-stream')
const pg = require('pg')
const slice = [].slice

module.exports = {
  Client,
  Pool,
  transactional,
  pg
}

/**
 * Pool
 */

function Pool(config) {
  if (!(this instanceof Pool)) {
    return new Pool(config)
  }

  this.config = config
}

Pool.prototype.connect = function() {
  return new Promise((resolve, reject) => {
    pg.connect(this.config, (error, client, done) => {
      if (error) {
        done(error)
        reject(error)
        return
      }

      resolve({ client, done })
    })
  })
}

Pool.prototype.query = function() {
  const args = slice.call(arguments)

  return this.connect().then((pool) => {
    return new Promise((resolve, reject) => {
      const cb = (error, result) => {
        pool.done()

        if (error) {
          reject(error)
        } else {
          resolve(result)
        }
      }

      args.push(cb)

      pool.client.query.apply(pool.client, args)
    })
  })
}

Pool.prototype.stream = function(text, value, options) {
  const stream = new PassThrough({
    objectMode: true
  })

  this.connect().then(pool => {
    const query = new QueryStream(text, value, options)
    const source = pool.client.query(query)
    source.on('end', cleanup)
    source.on('error', onError)
    source.on('close', cleanup)
    source.pipe(stream)

    function onError(err) {
      stream.emit('error', err)
      cleanup()
    }

    function cleanup() {
      pool.done()

      source.removeListener('end', cleanup)
      source.removeListener('error', onError)
      source.removeListener('close', cleanup)
    }
  }).catch(err => stream.emit('error', err))

  return stream
}

Pool.prototype.transactional = function(fn) {
  return this.connect().then((pool) => {
    function query() {
      let args = slice.call(arguments)
      return new Promise((resolve, reject) => {
        const cb = (error, result) => {
          if (error) {
            reject(error)
          } else {
            resolve(result)
          }
        }

        args.push(cb)

        pool.client.query.apply(pool.client, args)
      })
    }

    return transactional(query, pool.done, fn)
  })
}

/**
 * Client
 */
function Client(config) {
  if (!(this instanceof Client)) {
    return new Client(config)
  }

  this._client = new pg.Client(config)
  this._client.connect((error) => {
    if (error) {
      throw error
    }
  })
}

Client.prototype.query = function() {
  const args = slice.call(arguments)

  return new Promise((resolve, reject) => {
    const cb = (error, result) => {
      if (error) {
        reject(error)
      } else {
        resolve(result)
      }
    }

    args.push(cb)

    this._client.query.apply(this._client, args)
  })
}

Client.prototype.stream = function(text, value, options) {
  const stream = new PassThrough({
    objectMode: true
  })

  const query = new QueryStream(text, value, options)
  const source = this._client.query(query)
  source.on('end', cleanup)
  source.on('error', onError)
  source.on('close', cleanup)
  source.pipe(stream)

  function onError(err) {
    stream.emit('error', err)
    cleanup()
  }

  function cleanup() {
    source.removeListener('end', cleanup)
    source.removeListener('error', onError)
    source.removeListener('close', cleanup)
  }

  return stream
}

Client.prototype.transactional = function(fn) {
  let that = this

  function query() {
    const args = slice.call(arguments)

    return new Promise((resolve, reject) => {
      const cb = (error, result) => {
        if (error) {
          reject(error)
        } else {
          resolve(result)
        }
      }

      args.push(cb)

      that._client.query.apply(that._client, args)
    })
  }

  return transactional(query, () => {}, fn)
}

Client.prototype.end = function() {
  this._client.end()
}

/**
 * transactional
 */
function transactional(query, done, work) {
  const tx = new Transaction(query, done)
  return tx.begin()
    .then(() => {
      const res = work(tx)
      return Promise.resolve(res)
    })
    // Close and wait until all in-flight queries are done
    .then(
      res => {
        tx.close()
        return tx.allQueries()
          .then(() => Promise.resolve(res))
      },
      err => {
        tx.close()
        return tx.allQueries()
          .then(() => Promise.reject(err))
      }
    )
    .then(
      res => {
        let p = Promise.resolve()
        if (!tx._committed && !tx._rolledBack) {
          p = tx.commit()
        }
        return p
          .then(() => done())
          .then(() => Promise.resolve(res))
      },
      err => {
        let p = Promise.resolve()
        if (!tx._committed && !tx._rolledBack) {
          p = tx.rollback()
        }
        return p
          .then(() => done())
          .then(() => Promise.reject(err))
      }
    )
}

/**
 * Transaction
 */
function Transaction(query) {
  if (!(this instanceof Transaction)) {
    return new Transaction(query)
  }

  this._closed = false
  this._query = query
  this._committed = false
  this._rolledBack = false
  this._queriesInFlight = 0
  this._allQueriesPromise = null
  this._allQueriesResolve = null
}
Transaction.prototype.query = function() {
  if (this._closed) {
    throw new Error("Failed to perform a query on a closed transaction")
  }
  const args = slice.call(arguments)
  return this._performQuery.apply(this, args)
}
Transaction.prototype._performQuery = function() {
  const args = slice.call(arguments)

  const queryFinished = () => {
    console.log(`Query finished. ${this._queriesInFlight} => ${this._queriesInFlight - 1}`)
    this._queriesInFlight--
    if (this._queriesInFlight === 0 && this._allQueriesResolve) {
      console.log(`Resolving allQueries promise`)
      this._allQueriesResolve()
    }
  }

  console.log(`Query starting. ${this._queriesInFlight} => ${this._queriesInFlight + 1}`)
  this._queriesInFlight++
  let p = this._query.apply(null, args)
  p.then(
    queryFinished,
    queryFinished
  )
  return p
}
Transaction.prototype.allQueries = function() {
  if (this._allQueriesPromise) {
    return this._allQueriesPromise
  }

  console.log(`Creating allQueries promise`)
  this._allQueriesPromise = new Promise(resolve => {
    this._allQueriesResolve = resolve
    if (this._queriesInFlight === 0) {
      resolve()
    }
  })

  this._allQueriesPromise.then(() => console.log("allQueries promise resolved"))

  return this._allQueriesPromise
}
Transaction.prototype.close = function() {
  this._closed = true
}
Transaction.prototype.begin = function() {
  return this._query('BEGIN')
}
Transaction.prototype.commit = function() {
  this._committed = true
  return this._query('COMMIT')
}
Transaction.prototype.rollback = function() {
  this._rolledBack = true
  return this._query('ROLLBACK')
}
