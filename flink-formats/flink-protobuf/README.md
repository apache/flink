flink-protobuf
==============

This library adds support to flink for running sql against protobuf objects. Flink as of now
supports avro and json files backed by JsonSchema only. To add support for sql, flink needs to know 
the TypeInformation, this library provides TypeInformation for protobuf object.

It uses protobuf apis to retrieve fields and types of a prorobuf object and than provides the
field name, and type as a [PojoField](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/java/typeutils/PojoField.java) to flink.

Current limitations:

- In protobuf object field names have underscore at the end like `loggedAt_`, so in the sql it needs
to be referred as `loggedAt_` instead of `logged_at`. This should be fixable in flink apis, but 
would need some digging around in the code.

- Some fields are not supported yet like `Enum` etc, but should be trivial to add support.

With this it is possible to run a query like the following against a stream of protobuf objects

```sql
SELECT region_,
       count(*)
FROM people
WHERE currentAge_ > 40
  AND region_ IN ('SFO',
                 'BKN')
GROUP BY region_
```


