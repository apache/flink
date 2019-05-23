This project bundles the minimal dependencies from Hadoop's
FileSystem abstraction and shades them to avoid dependency conflicts.

This project is the basis for the bundled File System adapters
that are based on Hadoop code, but keep the appearance of Flink
being Hadoop-free, from a dependency perspective.

For this to work, however, we needed to adapt Hadoop's `Configuration`
class to load a (shaded) `core-default-shaded.xml` configuration with the
relocated class names of classes loaded via reflection.

# Changing the Hadoop Version

If you want to change the Hadoop version this project depends on, the following
steps are required to keep the shading correct:

1. from the respective Hadoop jar (currently 3.1.0),
  - copy `org/apache/hadoop/conf/Configuration.java` to `src/main/java/org/apache/hadoop/conf/` and
    - replace `core-default.xml` with `core-default-shaded.xml`.
  - copy `org/apache/hadoop/util/NativeCodeLoader.java` to `src/main/java/org/apache/hadoop/util/` and
    - replace the native method stubs as in the current setup (empty methods, or return false)
  - copy `core-default.xml` to `src/main/resources/core-default-shaded.xml` and
    - change every occurrence of `org.apache.hadoop` into `org.apache.flink.fs.shaded.hadoop3.org.apache.hadoop`
  - copy `core-site.xml` to `src/test/resources/core-site.xml` (as is)

2. verify the shaded jar:
  - does not contain any unshaded classes
  - all other classes should be under `org.apache.flink.fs.shaded.hadoop3`
  - there should be a `META-INF/services/org.apache.flink.core.fs.FileSystemFactory` file pointing to two classes: `org.apache.flink.fs.s3hadoop.S3FileSystemFactory` and `org.apache.flink.fs.s3hadoop.S3AFileSystemFactory`
  - other service files under `META-INF/services` should have their names and contents in the relocated `org.apache.flink.fs.s3hadoop.shaded` package
  - contains a `core-default-shaded.xml` file
  - does not contain a `core-default.xml` or `core-site.xml` file
