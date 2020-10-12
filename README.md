# ValueToTimestamp
Kafka Connect SMT to replace the record timestamp with a new timestamp from a field in the record value.

```
"transforms"='createTS',
"transforms.createTS.type"='kafka.connect.smt.ValueToTimestamp',
"transforms.createTS.field"='eventtime'
```
