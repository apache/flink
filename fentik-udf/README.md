# Build

```
mvn package

```

Will produce a jar file that needs to be copied into flink's lib/ directory

```
CREATE FUNCTION [IF NOT EXISTS] [catalog_name.][db_name.]function_name
  AS class_name [LANGUAGE JAVA|SCALA]

create function merge_multiset as 'com.fentik.MultisetToArray' language java
```
