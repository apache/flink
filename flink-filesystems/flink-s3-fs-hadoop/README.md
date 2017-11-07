This project is a wrapper around Hadoop's s3a file system. By pulling a smaller dependency tree and
shading all dependencies away, this keeps the appearance of Flink being Hadoop-free,
from a dependency perspective.

We also relocate the shaded Hadoop version to allow running in a different
setup. For this to work, however, we needed to adapt Hadoop's `Configuration`
class to load a (shaded) `core-default-shaded.xml` configuration with the
relocated class names of classes loaded via reflection
(in the fute, we may need to extend this to `mapred-default.xml` and `hdfs-defaults.xml` and their respective configuration classes).

# Changing the Hadoop Version

If you want to change the Hadoop version this project depends on, the following
steps are required to keep the shading correct:

1. copy `org/apache/hadoop/conf/Configuration.java` from the respective Hadoop jar file to this project
  - adapt the `Configuration` class by replacing `core-default.xml` with `core-default-shaded.xml`.
2. copy `core-default.xml` from the respective Hadoop jar file to this project as
  - `src/main/resources/core-default-shaded.xml` (replacing every occurence of `org.apache.hadoop` with `org.apache.flink.fs.s3hadoop.shaded.org.apache.hadoop`)
  - `src/test/resources/core-site.xml` (as is)
3. verify the shaded jar:
  - does not contain any unshaded classes except for `org.apache.flink.fs.s3hadoop.S3FileSystemFactory`
  - every other classes should be under `org.apache.flink.fs.s3hadoop.shaded`
  - there should be a `META-INF/services/org.apache.flink.fs.s3hadoop.S3FileSystemFactory` file pointing to the `org.apache.flink.fs.s3hadoop.S3FileSystemFactory` class
  - other service files under `META-INF/services` should have their names and contents in the relocated `org.apache.flink.fs.s3hadoop.shaded` package
  - contains a `core-default-shaded.xml` file
  - does not contain a `core-default.xml` or `core-site.xml` file
