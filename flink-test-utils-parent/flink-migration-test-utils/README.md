# Add State Migration Tests

This module collects tools that help to generate test data for the state migration tests.

The following dependency needs to be added to the module's Maven config in case a
migration test is meant to be added to that module:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>fink-migration-test-utils</artifactId>
    <version>${project.version}</version>
    <scope>test</scope>
</dependency>
```

and the following profile

```xml
<profile>
    <id>generate-migration-test-data</id>
    <build>
        <plugins>
            <plugin>
                <artifactId>maven-antrun-plugin</artifactId>
                <executions>
                    <execution>
                        <id>generate-migration-test-data</id>
                        <phase>package</phase>
                        <goals>
                            <goal>run</goal>
                        </goals>
                        <configuration>
                            <target>
                                <condition property="optional.classes" value="--classes '${generate.classes}'"
                                           else="">
                                    <isset property="generate.classes"/>
                                </condition>
                                <condition property="optional.prefixes"
                                           value="--prefixes '${generate.prefixes}'" else="">
                                    <isset property="generate.prefixes"/>
                                </condition>
                                <java classname="org.apache.flink.test.migration.MigrationTestsSnapshotGenerator"
                                      fork="true" failonerror="true" dir="${project.basedir}">
                                    <classpath refid="maven.test.classpath"/>
                                    <arg value="--dir"/>
                                    <arg line="${project.basedir}"/>
                                    <arg value="--version"/>
                                    <arg value="${generate.version}"/>
                                    <arg line="${optional.classes}"/>
                                    <arg line="${optional.prefixes}"/>
                                </java>
                            </target>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</profile>
```

To show the log during generating, add

```
logger.migration.name = org.apache.flink.test.migration
logger.migration.level = INFO
```

to the `log4j2-test.properties` of this module.

The state migration tests should satisfy

1. The tests are named like `*(Test|ITCase).(java|scala)`.
2. The test class name is the same with the file name.
3. The test implements `org.apache.flink.test.util.MigrationTest` and the snapshots generator methods are labeled
   with `@SnapshotsGenerator` or `@ParameterizedSnapshotsGenerator`.

# Generating Snapshots

To generate the snapshots for all the tests,
execute from within the target version's release branch:

```shell
mvn clean package -Pgenerate-migration-test-data -Dgenerate.version=1.17 -nsu -Dfast -DskipTests
```

The version (`1.17` in the command above) should be replaced with the target one.

By default, it will search for the migration tests under `src/test/java` and `src/test/scala`. It is also supported
to change the default search paths or only generate for specific classes:

```shell
# Change the default search paths
mvn clean package -Pgenerate-migration-test-data -Dgenerate.prefixes=xx,yy,zz -Dgenerate.version=1.17 -nsu -Dfast -DskipTests

# Generate for the specified classes
mvn clean package -Pgenerate-migration-test-data -Dgenerate.classes=class1,class2 -Dgenerate.version=1.17 -nsu -Dfast -DskipTests
```