#!/usr/bin/env node

const redis = require('redis')
const client = redis.createClient()

const log = console.log
const QUEUE_KEY = 'the:queue:key'
const POLL_TIMEOUT = 1 // seconds

function work(number) {
  let c = client.duplicate()
  log(`worker ${number} online`)
  let consumeQueue = () => {
    // log(`worker ${number} listening`)

    c.brpop(QUEUE_KEY, POLL_TIMEOUT, (err, response) => {
      if (err != null || response == null) {
        // log('got nothing:', err, response)
        setTimeout(consumeQueue, 0)
        return
      }

      let key = response[0]
      let json = response[1]
      let data = JSON.parse(json)
      log(`worker ${number} got ${data.data} from the queue ${key}`)
      log(`which was added at ${Date(data.ts)}`)
      setTimeout(consumeQueue, 0)
    })
  }
  consumeQueue()
}

function enqueue(data) {
  let c = client.duplicate()
  log(`adding ${data}`)
  let payload = JSON.stringify({
    ts: new Date().getTime(),
    data: data
  })
  return c.lpush(QUEUE_KEY, payload)
}

function main() {
  log('launching workers...')
  for (let i = 0; i < 4; i++) {
    setTimeout(() => work(i), 0)
  }
  log('...chilling...')
  setTimeout(() => {
    log('...loading the queue...')
    for (let i = 0; i < 20; i++) {
      enqueue(i + 1000)
    }
  }, 2000)
}

main()

setTimeout(() => {
  process.exit(0)
}, 5000)
