// Calls a Rust QUIC server with the TypeScript reference's client and prints what came back (the
// interop_typescript_client test in src/channel/quic/tests.rs runs it): bun tests/quic/client.ts <port> <cert.pem>
import { readFileSync } from 'node:fs'
import { Channel } from '../../../../../channel/quic'

const [port, cert] = process.argv.slice(2)
const client = new Channel()
  .post('hello', () => 'ts world')
  .connect(`quic://127.0.0.1:${port}`, { quic: { ca: readFileSync(cert) }, headers: async () => ({ 'X-Name': 'ts' }) })

const values: unknown[] = []
for await (const value of client.values('count')) values.push(value)
const { promise: event, resolve } = Promise.withResolvers()
const subscription = await client.subscribe('ticks', 'a', resolve)
await client.send('tick')
const result = {
  hello: await client.send('hello'),
  echo: await client.send('echo', { a: [1, 'b'] }),
  name: await client.send('name'),
  mirror: await client.send('mirror'),
  values,
  event: await event,
}
subscription.cancel()
client.stop()
console.log(JSON.stringify(result))
process.exit(0)
