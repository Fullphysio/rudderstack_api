// Captures test/fixtures/*.json by driving the REAL @rudderstack/rudder-sdk-node
// against a local HTTP server and recording what it sent, so the Dart tests
// compare against upstream behaviour rather than against what we believe it
// to be.
//
//   mkdir -p /tmp/rudder-ref && cd /tmp/rudder-ref && npm install @rudderstack/rudder-sdk-node@3.0.3
//   node /path/to/rudderstack_api/tool/capture_fixtures/capture.js
//
// `require('@rudderstack/rudder-sdk-node')` is resolved against the current
// working directory.

'use strict';

const fs = require('fs');
const http = require('http');
const path = require('path');
const zlib = require('zlib');

const NODE_SDK = '@rudderstack/rudder-sdk-node';
const NODE_SDK_VERSION = '3.0.3';
const WRITE_KEY = 'test-write-key';
const FIXTURES = path.join(__dirname, '..', '..', 'test', 'fixtures');

function loadSdk() {
  const resolved = require.resolve(NODE_SDK, { paths: [process.cwd()] });
  let dir = path.dirname(resolved);
  while (!fs.existsSync(path.join(dir, 'package.json')) || require(path.join(dir, 'package.json')).name !== NODE_SDK) {
    dir = path.dirname(dir);
  }
  const version = require(path.join(dir, 'package.json')).version;
  if (version !== NODE_SDK_VERSION) {
    throw new Error(`Installed ${NODE_SDK} is ${version}, expected ${NODE_SDK_VERSION}`);
  }
  return require(resolved);
}

const Analytics = loadSdk();

// The SDK flushes the very first event from inside track() without awaiting
// it, so a failed delivery surfaces as an unhandled rejection besides the
// error flush() rethrows. Node would otherwise abort the capture.
process.on('unhandledRejection', () => {});

function startServer(statuses) {
  const requests = [];
  let call = 0;
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      let raw = Buffer.concat(chunks);
      if (req.headers['content-encoding'] === 'gzip') raw = zlib.gunzipSync(raw);
      requests.push({ method: req.method, path: req.url, headers: req.headers, body: JSON.parse(raw.toString('utf8')) });
      const status = statuses[Math.min(call, statuses.length - 1)];
      call += 1;
      res.writeHead(status, { 'content-type': 'text/plain' });
      res.end(status === 200 ? 'OK' : 'error');
    });
  });
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve({ server, requests })));
}

function client(port, options) {
  return new Analytics(WRITE_KEY, Object.assign({
    dataPlaneUrl: `http://127.0.0.1:${port}/`,
    flushInterval: 600000,
    logLevel: 'none',
  }, options));
}

const fixedTimestamp = new Date('2026-01-31T09:05:03.042Z');

const calls = {
  identify: { userId: 'user-1', traits: { email: 'jane@example.com', plan: 'pro' }, context: { ip: '0.0.0.0' } },
  identify_context_traits_overridden: { userId: 'user-1', traits: { a: 1 }, context: { traits: { b: 2 } } },
  identify_anonymous: { anonymousId: 'anon-1' },
  track: {
    userId: 'user-1',
    event: 'Program Created',
    properties: { exercises: 3, tags: ['knee', 'acl'], nested: { ok: true }, empty: null },
    context: { traits: { role: 'physio' } },
    integrations: { All: false, PostHog: true },
    timestamp: fixedTimestamp,
  },
  track_date_property: { userId: 'user-1', event: 'E', properties: { at: fixedTimestamp } },
  page: { userId: 'user-1', name: 'Home', category: 'Docs', properties: { path: '/' } },
  screen: { anonymousId: 'anon-1', name: 'Settings', properties: { tab: 'billing' } },
  group: { userId: 'user-1', groupId: 'clinic-9', traits: { name: 'Clinic' } },
  alias: { userId: 'user-1', previousId: 'anon-1' },
};

async function captureMessages() {
  const out = {};
  for (const [name, input] of Object.entries(calls)) {
    const { server, requests } = await startServer([200]);
    const analytics = client(server.address().port);
    const type = name.split('_')[0];
    analytics[type](input);
    await analytics.flush();
    server.close();
    const request = requests[0];
    out[name] = {
      type,
      input: JSON.parse(JSON.stringify(input)),
      request: {
        method: request.method,
        path: request.path,
        headers: {
          authorization: request.headers.authorization,
          'content-encoding': request.headers['content-encoding'] || null,
          'user-agent': request.headers['user-agent'],
        },
        body: request.body,
      },
    };
  }
  return out;
}

async function captureValidation() {
  const cases = {
    track_without_identity: ['track', { event: 'E' }],
    track_without_event: ['track', { userId: 'u' }],
    track_empty_user_id: ['track', { userId: '', event: 'E' }],
    identify_without_identity: ['identify', { traits: { a: 1 } }],
    page_without_identity: ['page', { name: 'Home' }],
    screen_without_identity: ['screen', { name: 'Home' }],
    group_without_group_id: ['group', { userId: 'u' }],
    group_without_identity: ['group', { groupId: 'g' }],
    alias_without_user_id: ['alias', { previousId: 'p' }],
    alias_without_previous_id: ['alias', { userId: 'u' }],
  };
  const analytics = client(1);
  const out = {};
  for (const [name, [type, input]] of Object.entries(cases)) {
    try {
      analytics[type](input);
      out[name] = { type, input, threw: false };
    } catch (err) {
      out[name] = { type, input, threw: true, message: err.message };
    }
  }
  return out;
}

async function captureRetries() {
  const sequences = {
    success: [200],
    '500_then_success': [500, 200],
    '503_exhausts_retries': [503, 503, 503, 503, 503],
    '429_then_success': [429, 200],
    '502_twice_then_success': [502, 502, 200],
    '400_not_retried': [400, 200],
    '401_not_retried': [401, 200],
    '404_not_retried': [404, 200],
    '413_not_retried': [413, 200],
  };
  const out = {};
  for (const [name, statuses] of Object.entries(sequences)) {
    const { server, requests } = await startServer(statuses);
    const analytics = client(server.address().port);
    analytics.track({ userId: 'u', event: 'E' });
    let threw = false;
    try {
      await analytics.flush();
    } catch (err) {
      threw = true;
    }
    server.close();
    out[name] = {
      statuses,
      attempts: requests.length,
      threw,
      sameMessageIdOnEveryAttempt: new Set(requests.map((r) => r.body.batch[0].messageId)).size === 1,
    };
  }
  return out;
}

function write(name, data) {
  const file = path.join(FIXTURES, `${name}.json`);
  fs.writeFileSync(file, JSON.stringify({ capturedFrom: `${NODE_SDK}@${NODE_SDK_VERSION}`, cases: data }, null, 2) + '\n');
  console.log(`wrote ${path.relative(process.cwd(), file)}`);
}

(async () => {
  write('message_golden', await captureMessages());
  write('validation_golden', await captureValidation());
  write('retry_golden', await captureRetries());
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
