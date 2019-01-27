This project is a wrapper around the S3 file system from the Presto project which shades all dependencies.
Initial simple tests seem to indicate that it responds slightly faster
and in a bit more lightweight manner to write/read/list requests, compared
to the Hadoop s3a FS, but it has some semantic differences.

We also relocate the shaded Hadoop version to allow running in a different
setup. For this to work, however, we needed to adapt Hadoop's `Configuration`
class to load a (shaded) `core-default-shaded.xml` configuration with the
relocated class names of classes loaded via reflection
(in the future, we may need to extend this to `mapred-default.xml` and `hdfs-defaults.xml` and their respective configuration classes).

# Changing the Hadoop Version

If you want to change the Hadoop version this project depends on, the following
steps are required to keep the shading correct:

1. from the respective Hadoop jar (from the `com.facebook.presto.hadoop/hadoop-apache2` resource, currently version 2.7.3-1 as of our `pom.xml`),
  - copy `org/apache/hadoop/conf/Configuration.java` to `src/main/java/org/apache/hadoop/conf/` and
    - replace `core-default.xml` with `core-default-shaded.xml`.
  - copy `org/apache/hadoop/util/NativeCodeLoader.java` to `src/main/java/org/apache/hadoop/util/` and
    - replace the static initializer with
    ```
  static {
    LOG.info("Skipping native-hadoop library for flink-s3-fs-presto's relocated Hadoop... " +
             "using builtin-java classes where applicable");
  }
```
  - copy `core-default.xml` to `src/main/resources/core-default-shaded.xml` and
    - change every occurrence of `org.apache.hadoop` into `org.apache.flink.fs.s3presto.shaded.org.apache.hadoop`
  - copy `core-site.xml` to `src/test/resources/core-site.xml` (as is)
2. verify the shaded jar:
  - does not contain any unshaded classes except for `org.apache.flink.fs.s3presto.S3FileSystemFactory`
  - all other classes should be under `org.apache.flink.fs.s3presto.shaded`
  - there should be a `META-INF/services/org.apache.flink.fs.s3presto.S3FileSystemFactory` file pointing to the `org.apache.flink.fs.s3presto.S3FileSystemFactory` class
  - other service files under `META-INF/services` should have their names and contents in the relocated `org.apache.flink.fs.s3presto.shaded` package
  - contains a `core-default-shaded.xml` file
  - does not contain a `core-default.xml` or `core-site.xml` file
