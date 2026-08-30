#!/usr/bin/env node

/**
 * FluxDB High-Performance Benchmark Suite
 * Zero-dependency Node.js benchmarking tool for FluxDB.
 */

const net = require('net');
const { performance } = require('perf_hooks');

// Parse CLI Arguments
function parseArgs() {
  const args = {
    host: '127.0.0.1',
    port: 7379,
    auth: null,
    table: 'bench_table',
    requests: 50000,
    concurrency: 50,
    pipeline: 16,
    tests: ['set', 'get', 'json_set', 'scan', 'rank'],
    valueSize: 128,
  };

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '-h' || arg === '--host') args.host = argv[++i];
    else if (arg === '-P' || arg === '--port') args.port = parseInt(argv[++i], 10);
    else if (arg === '-a' || arg === '--auth' || arg === '--password') args.auth = argv[++i];
    else if (arg === '-t' || arg === '--table') args.table = argv[++i];
    else if (arg === '-n' || arg === '--requests') args.requests = parseInt(argv[++i], 10);
    else if (arg === '-c' || arg === '--concurrency') args.concurrency = parseInt(argv[++i], 10);
    else if (arg === '-p' || arg === '--pipeline') args.pipeline = parseInt(argv[++i], 10);
    else if (arg === '--tests') {
      const val = argv[++i].toLowerCase();
      args.tests = val === 'all' ? ['set', 'get', 'json_set', 'scan', 'rank', 'mixed'] : val.split(',').map(s => s.trim());
    } else if (arg === '--size') args.valueSize = parseInt(argv[++i], 10);
    else if (arg === '--help') {
      printHelp();
      process.exit(0);
    }
  }
  return args;
}

function printHelp() {
  console.log(`
⚡ FluxDB Benchmark Suite (Node.js)

Usage:
  node benchmark.js [options]

Options:
  -h, --host <host>          FluxDB Server host (default: 127.0.0.1)
  -P, --port <port>          FluxDB Server port (default: 7379)
  -a, --auth <password>      Authentication password (AUTH)
  -t, --table <table>        Target table name (default: bench_table)
  -n, --requests <num>       Total number of requests per test (default: 50000)
  -c, --concurrency <num>    Number of concurrent client connections (default: 50)
  -p, --pipeline <num>       Pipeline batch depth per connection (default: 16)
  --tests <list>             Comma-separated tests to run: set,get,json_set,scan,rank,mixed,all (default: set,get,json_set,scan,rank)
  --size <bytes>             Payload JSON padding size in bytes (default: 128)
  --help                     Display this help message

Examples:
  node benchmark.js -n 100000 -c 64 -p 32
  node benchmark.js --tests set,get -n 20000 -a secret123
`);
}

// Fast RESP Parser for pipelined responses
class RespParser {
  constructor() {
    this.buffer = Buffer.alloc(0);
  }

  feed(chunk) {
    if (this.buffer.length === 0) {
      this.buffer = chunk;
    } else {
      this.buffer = Buffer.concat([this.buffer, chunk]);
    }
  }

  next() {
    if (this.buffer.length === 0) return null;

    const prefix = this.buffer[0];
    const len = this.buffer.length;

    // Simple string (+), Error (-), Integer (:)
    if (prefix === 43 || prefix === 45 || prefix === 58) { // '+', '-', ':'
      const idx = this.buffer.indexOf(10); // '\n'
      if (idx === -1) return null;
      this.buffer = this.buffer.slice(idx + 1);
      return true;
    }

    // Bulk string ($)
    if (prefix === 36) { // '$'
      const idx = this.buffer.indexOf(10);
      if (idx === -1) return null;
      const strLen = parseInt(this.buffer.slice(1, idx).toString(), 10);
      if (strLen === -1) {
        this.buffer = this.buffer.slice(idx + 1);
        return true;
      }
      const totalNeed = idx + 1 + strLen + 2; // $len\r\n<data>\r\n
      if (len < totalNeed) return null;
      this.buffer = this.buffer.slice(totalNeed);
      return true;
    }

    // Array / Multi-bulk (*)
    if (prefix === 42) { // '*'
      const idx = this.buffer.indexOf(10);
      if (idx === -1) return null;
      const count = parseInt(this.buffer.slice(1, idx).toString(), 10);
      if (count === -1 || count === 0) {
        this.buffer = this.buffer.slice(idx + 1);
        return true;
      }
      // Skip array header
      this.buffer = this.buffer.slice(idx + 1);
      let elementsParsed = 0;
      while (elementsParsed < count) {
        if (this.buffer.length === 0) return null;
        const sub = this.next();
        if (sub === null) return null;
        elementsParsed++;
      }
      return true;
    }

    // Fallback: newline-delimited line
    const fallbackIdx = this.buffer.indexOf(10);
    if (fallbackIdx === -1) return null;
    this.buffer = this.buffer.slice(fallbackIdx + 1);
    return true;
  }
}

// Single Client Worker
class ClientWorker {
  constructor(host, port, auth) {
    this.host = host;
    this.port = port;
    this.auth = auth;
    this.socket = null;
    this.parser = new RespParser();
    this.pendingOps = 0;
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection({ host: this.host, port: this.port }, () => {
        this.socket.setNoDelay(true);
        if (this.auth) {
          this.executeRaw(`AUTH ${this.auth}\r\n`).then(() => resolve()).catch(reject);
        } else {
          resolve();
        }
      });
      this.socket.on('error', reject);
    });
  }

  executeRaw(rawCommand) {
    return new Promise((resolve, reject) => {
      const onData = (chunk) => {
        this.parser.feed(chunk);
        if (this.parser.next()) {
          this.socket.off('data', onData);
          resolve();
        }
      };
      this.socket.on('data', onData);
      this.socket.write(rawCommand);
    });
  }

  close() {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
  }
}

// Helper: Calculate Percentiles
function calculatePercentiles(latencies) {
  if (latencies.length === 0) return { avg: 0, min: 0, max: 0, p50: 0, p90: 0, p99: 0 };
  latencies.sort((a, b) => a - b);
  const sum = latencies.reduce((acc, v) => acc + v, 0);
  const count = latencies.length;
  return {
    avg: (sum / count).toFixed(3),
    min: latencies[0].toFixed(3),
    max: latencies[count - 1].toFixed(3),
    p50: latencies[Math.floor(count * 0.50)].toFixed(3),
    p90: latencies[Math.floor(count * 0.90)].toFixed(3),
    p99: latencies[Math.floor(count * 0.99)].toFixed(3),
  };
}

// Format numbers with commas
function formatNumber(num) {
  return num.toLocaleString();
}

// Main Benchmark Runner
async function runTest(testName, options, commandGenerator) {
  process.stdout.write(`\n🚀 Testing: \x1b[1;36m${testName.toUpperCase()}\x1b[0m ...\n`);

  const { host, port, auth, requests, concurrency, pipeline } = options;
  const workers = [];

  for (let i = 0; i < concurrency; i++) {
    const worker = new ClientWorker(host, port, auth);
    await worker.connect();
    workers.push(worker);
  }

  const requestsPerWorker = Math.floor(requests / concurrency);
  const totalActualRequests = requestsPerWorker * concurrency;
  const latencies = [];

  let completedRequests = 0;
  const startTime = performance.now();
  let lastPrintTime = startTime;

  const runWorker = (worker, workerId) => {
    return new Promise((resolve, reject) => {
      let sentCount = 0;
      let recvCount = 0;
      const startKeyIndex = workerId * requestsPerWorker;
      let batchStart = performance.now();

      const sendNextBatch = () => {
        if (sentCount >= requestsPerWorker) return;
        const batchSize = Math.min(pipeline, requestsPerWorker - sentCount);
        let batchPayload = '';
        batchStart = performance.now();

        for (let i = 0; i < batchSize; i++) {
          const globalId = startKeyIndex + sentCount + i;
          batchPayload += commandGenerator(globalId, totalActualRequests);
        }
        sentCount += batchSize;
        worker.socket.write(batchPayload);
      };

      const onData = (chunk) => {
        worker.parser.feed(chunk);
        while (worker.parser.next()) {
          recvCount++;
          completedRequests++;
          const opLatency = (performance.now() - batchStart) / pipeline;
          if (latencies.length < 50000) {
            latencies.push(opLatency);
          }

          const now = performance.now();
          if (now - lastPrintTime > 150) {
            const elapsedSec = (now - startTime) / 1000;
            const currentOps = (completedRequests / elapsedSec).toFixed(0);
            const progressPct = ((completedRequests / totalActualRequests) * 100).toFixed(1);
            process.stdout.write(`\r   ⏳ Progress: [${progressPct}%] ${formatNumber(completedRequests)}/${formatNumber(totalActualRequests)} reqs | Speed: \x1b[1;32m${formatNumber(parseInt(currentOps, 10))} ops/sec\x1b[0m `);
            lastPrintTime = now;
          }

          if (recvCount % pipeline === 0 || recvCount === sentCount) {
            sendNextBatch();
          }

          if (recvCount >= requestsPerWorker) {
            worker.socket.off('data', onData);
            resolve();
            return;
          }
        }
      };

      worker.socket.on('data', onData);
      worker.socket.on('error', reject);

      // Kickoff pipeline
      sendNextBatch();
    });
  };

  await Promise.all(workers.map((w, idx) => runWorker(w, idx)));

  const totalTimeSec = (performance.now() - startTime) / 1000;
  const throughput = Math.round(totalActualRequests / totalTimeSec);
  const stats = calculatePercentiles(latencies);

  // Close workers
  workers.forEach(w => w.close());

  process.stdout.write(`\r   ✅ Completed ${formatNumber(totalActualRequests)} requests in ${totalTimeSec.toFixed(2)}s (\x1b[1;32m${formatNumber(throughput)} req/sec\x1b[0m)\n`);
  console.log(`   📊 Latency: Avg: ${stats.avg}ms | Min: ${stats.min}ms | Max: ${stats.max}ms | P50: ${stats.p50}ms | P90: ${stats.p90}ms | P99: ${stats.p99}ms`);

  return {
    test: testName.toUpperCase(),
    opsSec: throughput,
    totalTime: totalTimeSec.toFixed(2),
    ...stats,
  };
}

async function main() {
  const options = parseArgs();

  console.log(`
========================================================================
   ⚡ FLUXDB BENCHMARK SUITE (Node.js High-Throughput Driver)
========================================================================
  Server Host:     ${options.host}:${options.port}
  Table Name:      ${options.table}
  Total Requests:  ${formatNumber(options.requests)} per test
  Concurrency:     ${options.concurrency} parallel clients
  Pipeline Depth:  ${options.pipeline} commands / batch
  Tests Selected:  ${options.tests.join(', ').toUpperCase()}
  Auth Password:   ${options.auth ? '******' : '(None)'}
========================================================================`);

  // Setup connection to prepare table and index
  const setupClient = new ClientWorker(options.host, options.port, options.auth);
  try {
    await setupClient.connect();
    console.log(`🔧 Preparing benchmark table '${options.table}'...`);
    await setupClient.executeRaw(`CREATE TABLE ${options.table}\r\n`).catch(() => {});
    await setupClient.executeRaw(`INDEX CREATE ${options.table} stats.score\r\n`).catch(() => {});
    setupClient.close();
  } catch (err) {
    console.error(`❌ Failed to connect to FluxDB at ${options.host}:${options.port} - ${err.message}`);
    process.exit(1);
  }

  const results = [];
  const padding = 'x'.repeat(Math.max(0, options.valueSize - 60));

  // Test 1: SET (Bulk Insert)
  if (options.tests.includes('set') || options.tests.includes('all')) {
    const res = await runTest('SET (Write)', options, (id) => {
      const key = `user_${String(id).padStart(8, '0')}`;
      const payload = JSON.stringify({ name: `player_${id}`, stats: { score: (id * 17) % 10000, level: (id % 100) + 1 }, pad: padding });
      return `SET ${options.table} ${key} ${payload}\r\n`;
    });
    results.push(res);
  }

  // Test 2: GET (Point Lookup)
  if (options.tests.includes('get') || options.tests.includes('all')) {
    const res = await runTest('GET (Read)', options, (id, total) => {
      const randomId = Math.floor(Math.random() * total);
      const key = `user_${String(randomId).padStart(8, '0')}`;
      return `GET ${options.table} ${key}\r\n`;
    });
    results.push(res);
  }

  // Test 3: JSON_SET (In-Place Update)
  if (options.tests.includes('json_set') || options.tests.includes('all')) {
    const res = await runTest('JSON_SET (Partial Update)', options, (id, total) => {
      const randomId = Math.floor(Math.random() * total);
      const key = `user_${String(randomId).padStart(8, '0')}`;
      const newScore = Math.floor(Math.random() * 50000);
      return `JSON_SET ${options.table} ${key} stats.score ${newScore}\r\n`;
    });
    results.push(res);
  }

  // Test 4: SCAN (Range Scan)
  if (options.tests.includes('scan') || options.tests.includes('all')) {
    const res = await runTest('SCAN (Range Scan)', options, (id, total) => {
      const startId = Math.floor(Math.random() * Math.max(1, total - 200));
      const startKey = `user_${String(startId).padStart(8, '0')}`;
      const endKey = `user_${String(startId + 100).padStart(8, '0')}`;
      return `SCAN ${options.table} ${startKey} ${endKey} 20\r\n`;
    });
    results.push(res);
  }

  // Test 5: RANK / TOP (Leaderboard Query)
  if (options.tests.includes('rank') || options.tests.includes('all')) {
    const res = await runTest('RANK (Leaderboard)', options, (id, total) => {
      if (id % 2 === 0) {
        return `TOP ${options.table} stats.score 10\r\n`;
      } else {
        const randomId = Math.floor(Math.random() * total);
        const key = `user_${String(randomId).padStart(8, '0')}`;
        return `RANK ${options.table} stats.score ${key}\r\n`;
      }
    });
    results.push(res);
  }

  // Test 6: MIXED (80% Read / 20% Write)
  if (options.tests.includes('mixed')) {
    const res = await runTest('MIXED (80% Read / 20% Write)', options, (id, total) => {
      const randomId = Math.floor(Math.random() * total);
      const key = `user_${String(randomId).padStart(8, '0')}`;
      if (Math.random() < 0.8) {
        return `GET ${options.table} ${key}\r\n`;
      } else {
        const payload = JSON.stringify({ name: `player_${randomId}`, stats: { score: Math.floor(Math.random() * 10000) } });
        return `SET ${options.table} ${key} ${payload}\r\n`;
      }
    });
    results.push(res);
  }

  // Print Summary Table
  console.log(`\n========================================================================================`);
  console.log(`   🏆 BENCHMARK SUMMARY RESULTS`);
  console.log(`========================================================================================`);
  console.log(`| Test Operation              | Throughput (ops/s) | Avg Latency | P50 (ms) | P99 (ms) |`);
  console.log(`|-----------------------------|--------------------|-------------|----------|----------|`);
  results.forEach(r => {
    const name = r.test.padEnd(27, ' ');
    const ops = formatNumber(r.opsSec).padStart(18, ' ');
    const avg = `${r.avg} ms`.padStart(11, ' ');
    const p50 = `${r.p50} ms`.padStart(8, ' ');
    const p99 = `${r.p99} ms`.padStart(8, ' ');
    console.log(`| ${name} | ${ops} | ${avg} | ${p50} | ${p99} |`);
  });
  console.log(`========================================================================================\n`);
}

main().catch(err => {
  console.error('Fatal benchmark error:', err);
  process.exit(1);
});
