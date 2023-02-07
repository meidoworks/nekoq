Number Generator
================

### 1. API

```
GET /:gen_key/:count

gen_key: The name of id generator
count: total expected id count

Response:
Hex string of 128-bit ID in lower case
IDs are splitted by '\n', e.g.
0023e344453a00040000000200000000
0023e344453a00040000000200000001
0023e344453a00040000000200000002
Read until EOF of the http body(no '\n' for the last ID).
```

### 2. Note

* Currently, if the service is restarted, the elementId(part of the id format) will not be guaranteed the same if the
  same gen_key is provided

### 3. Pending items

* [ ] Gen_key and element_id pair persistent
