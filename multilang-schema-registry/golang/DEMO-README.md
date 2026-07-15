# GSR Go Client — Interop Demo

## Commands

**1. Refresh AWS creds** (skip if `aws sts get-caller-identity` already works):

```bash
ada credentials update --account 850995546034 --role Admin --provider isengard --once
```

**2. Run the demo** (captures start/end timestamps for the CloudTrail lookup):

```bash
export DEMO_START=$(date -u +%Y-%m-%dT%H:%M:%SZ) && \
  cd /workplace/mrknox/phase-8-perf/multilang-schema-registry/golang && \
  AWS_PROFILE=default make demo-interop 2>&1 | tee demo.log ; \
  export DEMO_END=$(date -u +%Y-%m-%dT%H:%M:%SZ) && \
  echo "DEMO_START=$DEMO_START  DEMO_END=$DEMO_END"
```

**3. Wait for CloudTrail delivery, then show the pivot table:**

```bash
echo "waiting 90s for CloudTrail delivery..." && sleep 90 && \
aws cloudtrail lookup-events \
  --region us-east-2 \
  --lookup-attributes AttributeKey=EventSource,AttributeValue=glue.amazonaws.com \
  --start-time "$DEMO_START" \
  --end-time "$DEMO_END" \
  --max-items 200 \
  --query 'Events[].CloudTrailEvent' --output text \
  | tr '\t' '\n' \
  | python3 -c "
import sys, json
from collections import defaultdict
from datetime import datetime

WRITE_OPS = {'CreateSchema', 'RegisterSchemaVersion'}
READ_OPS  = {'GetSchemaVersion', 'GetSchemaByDefinition'}
META_OPS  = {'PutSchemaVersionMetadata'}
OP_ABBREV = {'CreateSchema':'Create','RegisterSchemaVersion':'Register','GetSchemaByDefinition':'GetByDef','GetSchemaVersion':'GetVer','PutSchemaVersionMetadata':'PutMeta'}

events = []
version_to_name = {}
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    e = json.loads(line)
    ua = e.get('userAgent','')
    if 'glue-schema-registry-go' in ua: client = 'go'
    elif 'app/kafka' in ua:             client = 'java'
    else:                                continue
    req  = e.get('requestParameters') or {}
    resp = e.get('responseElements')  or {}
    events.append((e, client, req, resp))
    name, vid = None, None
    if e['eventName'] == 'CreateSchema':
        name, vid = req.get('schemaName'), resp.get('schemaVersionId')
    elif e['eventName'] == 'RegisterSchemaVersion':
        name, vid = (req.get('schemaId') or {}).get('schemaName'), resp.get('schemaVersionId')
    elif e['eventName'] == 'GetSchemaByDefinition':
        name, vid = (req.get('schemaId') or {}).get('schemaName'), resp.get('schemaVersionId')
    elif e['eventName'] == 'PutSchemaVersionMetadata':
        mkv = req.get('metadataKeyValue') or {}
        if mkv.get('metadataKey') == 'x-amz-meta-transport':
            name = mkv.get('metadataValue')
        vid = req.get('schemaVersionId')
    if name and vid: version_to_name[vid] = name

by_version = defaultdict(list)
for e, client, req, resp in events:
    op = e['eventName']
    if op not in WRITE_OPS | READ_OPS | META_OPS: continue
    schema = req.get('schemaId') or {}
    vid = schema.get('schemaVersionId') or req.get('schemaVersionId') or resp.get('schemaVersionId')
    if not vid: continue
    by_version[vid].append({'time': e['eventTime'], 'client': client, 'op': op})

def parse_ts(s): return datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ')

rows = []
for vid, evs in by_version.items():
    evs.sort(key=lambda x: x['time'])
    t0 = parse_ts(evs[0]['time'])
    writes = [e for e in evs if e['op'] in WRITE_OPS]
    reads  = [e for e in evs if e['op'] in READ_OPS]
    writer_clients = sorted({w['client'] for w in writes})
    reader_clients = sorted({r['client'] for r in reads})
    cross = '🔁' if writer_clients and reader_clients and set(writer_clients) != set(reader_clients) else '  '
    tl_parts = []
    for e in evs:
        dt = int((parse_ts(e['time']) - t0).total_seconds())
        tl_parts.append(f'[+{dt:>2}s {e[\"client\"]}:{OP_ABBREV.get(e[\"op\"], e[\"op\"])}]')
    rows.append({
        'schemaName':  version_to_name.get(vid, '(unknown)'),
        'versionId':   vid[:8],
        'writers':     ','.join(writer_clients) or '-',
        'readers':     ','.join(reader_clients) or '-',
        'cross':       cross,
        'events':      len(evs),
        'started':     evs[0]['time'][11:19],
        'timeline':    ' '.join(tl_parts),
    })
rows.sort(key=lambda r: r['started'])

if not rows: print('no events'); sys.exit(0)

fixed = ['schemaName','versionId','writers','readers','cross','events','started']
widths = {c: max(len(c), max(len(str(r[c])) for r in rows)) for c in fixed}
hdr = '  '.join(f'{c:<{widths[c]}}' for c in fixed) + '  timeline'
print(hdr)
print('-' * len(hdr))
for r in rows:
    left = '  '.join(f'{str(r[c]):<{widths[c]}}' for c in fixed)
    print(f'{left}  {r[\"timeline\"]}')
"
```

---

## What the demo does

Proves the Go GSR client is wire-format compatible with the Java GSR client by running both against the same real AWS Glue registry and exchanging bytes through a real Kafka broker. **21 scenarios**, all must pass, cleans up on exit.

## The 21 scenarios

| # | Scenario | Producer | Consumer | Format | Compression | What it proves |
|---|---|---|---|---|---|---|
| 1 | **Go-writes / Java-reads** | **Go (auto-registers)** | **Java (cold cache)** | AVRO | NONE | Go client is the schema-writer; Java resolves UUID cold from Glue |
| 2 | Same-version | Go | Java | PROTOBUF | NONE | Baseline sanity |
| 3 | Same-version | Java | Go | PROTOBUF | NONE | Baseline sanity |
| 4 | Same-version | Go | Java | JSON | NONE | Baseline sanity |
| 5 | Same-version | Java | Go | JSON | NONE | Baseline sanity |
| 6 | Same-version | Go | Java | AVRO | NONE | Baseline sanity (no evolution) |
| 7 | Same-version | Java | Go | AVRO | NONE | Baseline sanity (no evolution) |
| 8 | Auto-register | Go | Go | AVRO | NONE | Schema deleted mid-flight → client auto-recreates on next encode |
| 9 | Cache behavior | Go | Go | AVRO | NONE | Client-side cache: miss → hit → eviction → re-fetch |
| 10 | Cross-version B | Go v1 | Java v2 | PROTOBUF | ZLIB | Same + compression |
| 11 | Cross-version B | Go v1 | Java v2 | PROTOBUF | NONE | Go-Protobuf → Java |
| 12 | Cross-version B | Go v1 | Java v2 | JSON | ZLIB | Same + compression |
| 13 | Cross-version B | Go v1 | Java v2 | JSON | NONE | Go-JSON → Java |
| 14 | Cross-version B | Go v1 | Java v2 | AVRO | ZLIB | Same + compression |
| 15 | Cross-version B | Go v1 | Java v2 | AVRO | NONE | Go writer → Java reader-schema projection |
| 16 | Cross-version A | Java v1 | Go v2 | PROTOBUF | ZLIB | Same + compression |
| 17 | Cross-version A | Java v1 | Go v2 | PROTOBUF | NONE | Java-Protobuf → Go |
| 18 | Cross-version A | Java v1 | Go v2 | JSON | ZLIB | Same + compression |
| 19 | Cross-version A | Java v1 | Go v2 | JSON | NONE | Java-JSON → Go |
| 20 | Cross-version A | Java v1 | Go v2 | AVRO | ZLIB | Same + compression |
| 21 | Cross-version A | Java v1 | Go v2 | AVRO | NONE | Java writer → Go reader-schema projection |

## How it runs (in order)

1. Starts a Kafka broker in Docker via testcontainers-go
2. Starts a Java sidecar HTTP service that wraps the real Java GSR client (`GlueSchemaRegistryKafkaSerializer` / `Deserializer`)
3. Resolves your AWS account via STS, prints a banner
4. For each scenario:
   - Registers schema(s) in real Glue (via Java sidecar OR Go client)
   - Encodes a record on one side (hex-dumps the GSR wire bytes)
   - Produces to Kafka
   - Consumes on the other side, decodes, asserts field-by-field equality
   - Records PASS/FAIL + schema version UUID
5. Prints a Markdown results table (21 rows)
6. Deletes every `demo-4.15-*` schema from Glue
7. Exits 0 if all pass, non-zero otherwise

## Wire format under test

Every message on Kafka: `0x03 header` + `compression byte` + `16-byte schema UUID` + `payload`. Both languages produce and consume this identically — that's the interop guarantee.

## Runtime

~5 minutes end-to-end. Cost: ~40 Glue schemas × fractions of a cent each.
