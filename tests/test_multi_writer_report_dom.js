const fs = require('node:fs');
const vm = require('node:vm');

class Element {
  constructor(tag = 'div', value = '') {
    this.tag = tag;
    this.value = value;
    this.textContent = '';
    this.children = [];
    this.listeners = {};
  }
  append(...items) { this.children.push(...items); }
  appendChild(item) { this.children.push(item); return item; }
  replaceChildren(...items) { this.children = items; }
  addEventListener(name, callback) { this.listeners[name] = callback; }
}

const html = fs.readFileSync(process.argv[2], 'utf8');
const dataMatch = html.match(/<script id="benchmark-data" type="application\/json">([\s\S]*?)<\/script>/);
if (!dataMatch) throw new Error('missing embedded data');
const scripts = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)];
const appScript = scripts.at(-1)[1];
const elements = new Map();
for (const id of ['benchmark-data', 'historical-context', 'search', 'kind', 'study', 'profile', 'phase', 'dataset', 'count', 'rows', 'streaming-rows']) {
  elements.set(id, new Element(id, id === 'kind' ? 'candidate_lane' : ''));
}
elements.get('benchmark-data').textContent = dataMatch[1];
// Mirror the two static kind options rendered by the report document.
elements.get('kind').children.push(Object.assign(new Element('option'), { value: '' }));
elements.get('kind').children.push(Object.assign(new Element('option'), { value: 'candidate_lane' }));
const document = {
  getElementById: id => {
    if (!elements.has(id)) elements.set(id, new Element(id));
    return elements.get(id);
  },
  createElement: tag => new Element(tag),
};
vm.runInNewContext(appScript, { document, JSON, String, Object, Set });
const rows = () => elements.get('rows').children;
const streams = () => elements.get('streaming-rows').children;
function assert(condition, message) { if (!condition) throw new Error(message); }

assert(rows().length === 1, 'default filter should show candidate lane data only');
assert(rows()[0].children[7].textContent === '123.5 item operations/second', 'candidate flat throughput is rendered');
assert(rows()[0].children[8].textContent === '7.2 milliseconds', 'candidate flat p99 is rendered');

elements.get('kind').value = '';
elements.get('kind').listeners.change();
const historical = rows().find(row => row.children[1].textContent === 'historical_http');
const protocol = rows().find(row => row.children[1].textContent === 'protocol_phase');
const matched = rows().find(row => row.children[1].textContent === 'matched_version_lane_1');
assert(historical && historical.children[7].textContent === '88 operations/s', 'historical HTTP flat rate is rendered');
assert(protocol && protocol.children[7].textContent === '10 → 12 operations/s', 'protocol before/after values are rendered');
assert(matched && matched.children[7].textContent === '75 item operations/second' && matched.children[8].textContent === '9.1 milliseconds', 'matched-version flat throughput and p99 are rendered');
assert(streams().length === 1 && streams()[0].children[7].textContent === '42.5 MiB/s', 'streaming rate and unit are rendered in separate table');

elements.get('kind').value = 'historical_http';
elements.get('kind').listeners.change();
assert(rows().length === 1 && streams().length === 0, 'kind filter isolates historical HTTP rows');
elements.get('kind').value = 'streaming';
elements.get('kind').listeners.change();
assert(rows().length === 0 && streams().length === 1, 'kind filter exposes streaming rows');
console.log('DOM report checks passed');
