// Generates the golden files in ../golden from the JS SDK, which serves as the oracle.
//
// Usage (Node 22+):
//   (cd $APIFY_SDK_JS_DIR && pnpm install && pnpm compile)
//   APIFY_SDK_JS_DIR=/path/to/apify-sdk-js node --no-warnings conformance/oracle/generate-golden.mts
//
// APIFY_SDK_JS_DIR is a checkout of apify/apify-sdk-js, compiled to `dist/` with its own `tsc`
// (its sources re-export type aliases, which per-file transpilers cannot strip).
// `tests/golden.rs` replays every case; a divergence either is a bug or must be listed in
// docs/allowed-differences.md.

import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = process.env.APIFY_SDK_JS_DIR;
if (!root) throw new Error('Set APIFY_SDK_JS_DIR to a checkout of apify/apify-sdk-js with dependencies installed.');

const source = async (path: string) => import(pathToFileURL(join(root, 'dist', path)).href);

const { Configuration } = await source('configuration.js');
const { ChargingManager } = await source('charging.js');
const { ProxyConfiguration } = await source('proxy_configuration.js');
const { parseInputValue } = await source('utils.js');
const { getDefaultsFromInputSchema } = await source('input-schemas.js');
const { purlToRegExp } = await import(pathToFileURL(join(root, 'node_modules', '@apify', 'pseudo_url', 'index.js')).href);

const goldenDir = join(dirname(fileURLToPath(import.meta.url)), '..', 'golden');
const write = (name: string, cases: unknown) => {
    writeFileSync(join(goldenDir, `${name}.json`), `${JSON.stringify(cases, null, 2)}\n`);
    console.log(`${name}: ${(cases as unknown[]).length} cases`);
};

// No crawlee.json may influence the configuration.
process.chdir(mkdtempSync(join(tmpdir(), 'apify-oracle-')));
const originalEnv = { ...process.env };
function withEnv<T>(env: Record<string, string>, fn: () => T): T {
    for (const key of Object.keys(process.env)) {
        if (/^(APIFY_|ACTOR_|CRAWLEE_)/.test(key)) delete process.env[key];
    }
    Object.assign(process.env, env);
    try {
        return fn();
    } finally {
        process.env = { ...originalEnv };
    }
}

// ─── Configuration ──────────────────────────────────────────────────────────

const CONFIGURATION_FIELDS = [
    'token', 'apiBaseUrl', 'apiPublicBaseUrl', 'defaultDatasetId', 'defaultKeyValueStoreId', 'defaultRequestQueueId',
    'inputKey', 'metamorphAfterSleepMillis', 'actorEventsWsUrl', 'actorId', 'actorRunId', 'actorTaskId',
    'containerPort', 'containerUrl', 'standbyPort', 'standbyUrl', 'proxyHostname', 'proxyPassword', 'proxyPort',
    'proxyStatusUrl', 'isAtHome', 'userId', 'userIsPaying', 'actorPermissionLevel', 'maxTotalChargeUsd',
    'metaOrigin', 'testPayPerEvent', 'useChargingLogDataset', 'actorPricingInfo', 'chargedEventCounts',
    'actorStoragesJson', 'inputSecretsPrivateKeyFile', 'inputSecretsPrivateKeyPassphrase', 'memoryMbytes',
    'availableMemoryRatio', 'persistStateIntervalMillis', 'purgeOnStart', 'persistStorage', 'storageDir',
];

const configurationCases: Record<string, string>[] = [
    {},
    { APIFY_IS_AT_HOME: '1', ACTOR_MEMORY_MBYTES: '4096', ACTOR_RUN_ID: 'run', APIFY_TOKEN: 'secret', APIFY_USER_IS_PAYING: '1' },
    { APIFY_IS_AT_HOME: '1', CRAWLEE_AVAILABLE_MEMORY_RATIO: '0.5' },
    { APIFY_IS_AT_HOME: 'false', APIFY_USER_IS_PAYING: 'false' },
    { APIFY_IS_AT_HOME: '0', APIFY_AVAILABLE_MEMORY_RATIO: '0.8', CRAWLEE_AVAILABLE_MEMORY_RATIO: '0.3' },
    {
        ACTOR_DEFAULT_DATASET_ID: 'actor-ds', APIFY_DEFAULT_DATASET_ID: 'apify-ds',
        APIFY_DEFAULT_KEY_VALUE_STORE_ID: 'apify-kvs', ACTOR_INPUT_KEY: '', APIFY_INPUT_KEY: 'APIFY_INPUT',
        CRAWLEE_INPUT_KEY: 'CRAWLEE_INPUT', ACTOR_EVENTS_WEBSOCKET_URL: 'ws://events',
        APIFY_ACTOR_EVENTS_WS_URL: 'ws://legacy', APIFY_ACTOR_ID: 'legacy-actor', ACTOR_TASK_ID: 'task',
    },
    { CRAWLEE_INPUT_KEY: 'CRAWLEE_INPUT', APIFY_MEMORY_MBYTES: '2048', CRAWLEE_MEMORY_MBYTES: '1024' },
    { ACTOR_MAX_TOTAL_CHARGE_USD: '0' },
    { ACTOR_MAX_TOTAL_CHARGE_USD: '12.5', ACTOR_TEST_PAY_PER_EVENT: 'true', ACTOR_USE_CHARGING_LOG_DATASET: '1' },
    {
        APIFY_PROXY_HOSTNAME: 'proxy.local', APIFY_PROXY_PORT: '9000', APIFY_PROXY_PASSWORD: 'pw',
        APIFY_PROXY_STATUS_URL: 'http://status', APIFY_API_BASE_URL: 'http://api', APIFY_API_PUBLIC_BASE_URL: 'http://pub',
    },
    {
        APIFY_PERSIST_STATE_INTERVAL_MILLIS: '1000', CRAWLEE_PERSIST_STATE_INTERVAL_MILLIS: '5000',
        APIFY_PURGE_ON_START: '0', CRAWLEE_PERSIST_STORAGE: 'false', CRAWLEE_STORAGE_DIR: '/tmp/s',
    },
    { APIFY_TEST_PERSIST_INTERVAL_MILLIS: '2000', ACTOR_WEB_SERVER_PORT: '8080', APIFY_CONTAINER_URL: 'http://c' },
    { APIFY_METAMORPH_AFTER_SLEEP_MILLIS: '10', ACTOR_STANDBY_PORT: '5000', ACTOR_STANDBY_URL: 'http://standby' },
];

write(
    'configuration',
    configurationCases.map((env) => ({
        env,
        expected: withEnv(env, () => {
            const config = new Configuration();
            return Object.fromEntries(CONFIGURATION_FIELDS.map((field) => [field, config[field] ?? null]));
        }),
    })),
);

// ─── Charging ───────────────────────────────────────────────────────────────

type Operation =
    | { op: 'charge'; eventName: string; count: number }
    | { op: 'pushDataLimit'; itemsCount: number; eventName?: string; isDefaultDataset: boolean }
    | { op: 'maxCount'; eventName: string };

const pricing = (events: Record<string, number>) =>
    JSON.stringify({
        pricingModel: 'PAY_PER_EVENT',
        pricingPerEvent: {
            actorChargeEvents: Object.fromEntries(
                Object.entries(events).map(([name, price]) => [name, { eventTitle: name, eventPriceUsd: price }]),
            ),
        },
    });

const chargingCases: { name: string; env: Record<string, string>; operations: Operation[] }[] = [
    {
        name: 'budget with the default dataset item event',
        env: {
            APIFY_IS_AT_HOME: '1', ACTOR_RUN_ID: 'run', ACTOR_MAX_TOTAL_CHARGE_USD: '10',
            APIFY_ACTOR_PRICING_INFO: pricing({ page: 1, 'apify-default-dataset-item': 0.5 }),
            APIFY_CHARGED_ACTOR_EVENT_COUNTS: JSON.stringify({ page: 3 }),
        },
        operations: [
            { op: 'maxCount', eventName: 'page' },
            { op: 'maxCount', eventName: 'unknown' },
            { op: 'pushDataLimit', itemsCount: 10, eventName: 'page', isDefaultDataset: true },
            { op: 'pushDataLimit', itemsCount: 3, eventName: 'page', isDefaultDataset: true },
            { op: 'pushDataLimit', itemsCount: 30, isDefaultDataset: true },
            { op: 'charge', eventName: 'page', count: 2 },
            { op: 'charge', eventName: 'apify-default-dataset-item', count: 20 },
            { op: 'charge', eventName: 'page', count: 1 },
            { op: 'pushDataLimit', itemsCount: 5, isDefaultDataset: true },
            { op: 'charge', eventName: 'unknown', count: 4 },
        ],
    },
    {
        name: 'prices that do not add up in binary',
        env: {
            APIFY_IS_AT_HOME: '1', ACTOR_RUN_ID: 'run', ACTOR_MAX_TOTAL_CHARGE_USD: '0.3',
            APIFY_ACTOR_PRICING_INFO: pricing({ tenth: 0.1, third: 0.0333333 }),
            APIFY_CHARGED_ACTOR_EVENT_COUNTS: JSON.stringify({}),
        },
        operations: [
            { op: 'charge', eventName: 'tenth', count: 1 },
            { op: 'charge', eventName: 'tenth', count: 1 },
            { op: 'maxCount', eventName: 'tenth' },
            { op: 'maxCount', eventName: 'third' },
            { op: 'charge', eventName: 'third', count: 5 },
            { op: 'charge', eventName: 'tenth', count: 1 },
        ],
    },
    {
        name: 'no budget',
        env: {
            APIFY_IS_AT_HOME: '1', ACTOR_RUN_ID: 'run',
            APIFY_ACTOR_PRICING_INFO: pricing({ page: 0.01 }),
            APIFY_CHARGED_ACTOR_EVENT_COUNTS: JSON.stringify({ page: 100 }),
        },
        operations: [
            { op: 'maxCount', eventName: 'page' },
            { op: 'charge', eventName: 'page', count: 1000 },
            { op: 'pushDataLimit', itemsCount: 7, eventName: 'page', isDefaultDataset: false },
        ],
    },
    {
        name: 'local test pay-per-event: every event costs 1',
        env: { ACTOR_TEST_PAY_PER_EVENT: '1', ACTOR_MAX_TOTAL_CHARGE_USD: '3' },
        operations: [
            { op: 'maxCount', eventName: 'anything' },
            { op: 'pushDataLimit', itemsCount: 5, eventName: 'page', isDefaultDataset: true },
            { op: 'charge', eventName: 'page', count: 2 },
            { op: 'charge', eventName: 'page', count: 2 },
            { op: 'charge', eventName: 'page', count: 1 },
        ],
    },
    {
        name: 'not pay-per-event',
        env: { APIFY_IS_AT_HOME: '1', ACTOR_RUN_ID: 'run', APIFY_ACTOR_PRICING_INFO: '{"pricingModel":"FREE"}', APIFY_CHARGED_ACTOR_EVENT_COUNTS: '{}' },
        operations: [
            { op: 'charge', eventName: 'page', count: 3 },
            { op: 'pushDataLimit', itemsCount: 5, isDefaultDataset: true },
        ],
    },
];

const stubClient = { run: () => ({ charge: async () => {}, get: async () => undefined }) };
const chargingResults = [];
for (const { name, env, operations } of chargingCases) {
    const results = await withEnv(env, async () => {
        const manager = new ChargingManager(new Configuration(), stubClient);
        await manager.init();
        const out = [];
        for (const operation of operations) {
            if (operation.op === 'charge') {
                out.push(await manager.charge({ eventName: operation.eventName, count: operation.count }));
            } else if (operation.op === 'pushDataLimit') {
                const { itemsCount, eventName, isDefaultDataset } = operation;
                out.push(manager.calculatePushDataLimit(itemsCount, { eventName, isDefaultDataset }));
            } else {
                out.push(manager.calculateMaxEventChargeCountWithinLimit(operation.eventName));
            }
        }
        return out;
    });
    chargingResults.push({ name, env, operations, expected: JSON.parse(JSON.stringify(results)) });
}
write('charging', chargingResults);

// ─── Proxy usernames ────────────────────────────────────────────────────────

const proxyCases = [
    {},
    { groups: ['RESIDENTIAL'] },
    { groups: ['RESIDENTIAL', 'GOOGLE_SERP'], countryCode: 'US' },
    { apifyProxyGroups: ['A_B'], apifyProxyCountry: 'CZ', apifyProxySubdivision: 'PR' },
    { countryCode: 'US', subdivisionCode: 'CA', password: 'p@ss:w/rd' },
];
write(
    'proxy',
    proxyCases.map((options) =>
        withEnv({ APIFY_PROXY_PASSWORD: 'env-password' }, () => {
            const proxy = new ProxyConfiguration(options, new Configuration());
            return { options, sessionId: 'abc_123', expected: proxy.composeDefaultUrl('abc_123') };
        }),
    ),
);

// ─── Pseudo-URLs ────────────────────────────────────────────────────────────

const purlCases = [
    ['http://www.example.com/pages/[(\\w|-)*]', ['http://www.example.com/pages/', 'http://www.example.com/pages/my-page', 'HTTP://WWW.EXAMPLE.COM/PAGES/X', 'http://www.example.com/pages/a/b', 'http://wwwXexample.com/pages/']],
    ['http://www.example.com/search?do[\\x5B]load[\\x5D]=1', ['http://www.example.com/search?do[load]=1', 'http://www.example.com/search?doXloadX=1']],
    ['https://shop.dev/product/[\\d+]', ['https://shop.dev/product/42', 'https://shop.dev/product/42/', 'https://shop.dev/product/']],
    ['https://a.dev/[.*]/detail?id=[[0-9]+]', ['https://a.dev/x/y/detail?id=17', 'https://a.dev//detail?id=', 'https://a.dev/x/detail?id=a']],
    ['  https://trim.dev/  ', ['https://trim.dev/', ' https://trim.dev/']],
];
write(
    'pseudo_urls',
    purlCases.map(([purl, urls]) => {
        const regex = purlToRegExp(purl as string);
        return { purl, matches: (urls as string[]).map((url) => [url, regex.test(url)]) };
    }),
);

// ─── Input values and schema defaults ───────────────────────────────────────

const inputCases: [string, string | null][] = [
    ['{"a":1}', 'application/json; charset=utf-8'],
    ['{"a":1}', 'application/octet-stream'],
    ['not json', 'application/octet-stream'],
    ['hello', 'text/plain'],
    ['[1,2]', 'APPLICATION/JSON'],
];
write(
    'input_values',
    inputCases.map(([value, contentType]) => {
        const parsed = parseInputValue(Buffer.from(value), contentType);
        const expected = Buffer.isBuffer(parsed) ? { binary: parsed.toString() } : { value: parsed };
        return { value, contentType, expected };
    }),
);

const schemaCases = [
    [{ properties: { a: { type: 'integer', default: 1 }, b: { type: 'string' }, c: { type: 'object', default: { x: 1 } } } }, { b: 'given', c: { y: 2 } }],
    [{ properties: { list: { type: 'array', default: [] }, flag: { type: 'boolean', default: false } } }, { flag: true, extra: 1 }],
];
write(
    'schema_defaults',
    schemaCases.map(([schema, input]) => ({
        schema,
        input,
        expected: { ...getDefaultsFromInputSchema(schema), ...(input as object) },
    })),
);

// ─── Number(x.toFixed(n)) ───────────────────────────────────────────────────

const toFixedCases: [number, number][] = [
    [0.25, 1], [0.35, 1], [1.005, 2], [2.675, 2], [4.99999999999, 4], [0.1 + 0.2, 6], [1e-7, 6], [5e-7, 6],
    [123.456789, 0], [-1.5, 0], [-0.25, 1], [9.9999995, 6], [0.0333333 * 3, 6], [10 / 3, 4], [1e20, 2],
];
write(
    'to_fixed',
    toFixedCases.map(([x, digits]) => ({ x, digits, expected: Number(x.toFixed(digits)) })),
);
