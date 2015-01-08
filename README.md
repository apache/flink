# Apache Flink


Apache Flink is an open source system for expressive, declarative, fast, and efficient data analysis. Flink combines the scalability and programming flexibility of distributed MapReduce-like platforms with the efficiency, out-of-core execution, and query optimization capabilities found in parallel databases.

Learn more about Flink at [http://flink.apache.org/](http://flink.apache.org/)

## Build Apache Flink From Source


### Requirements
* Unix-like environment (We use Linux, Mac OS X, Cygwin)
* git
* Maven (at least version 3.0.4)
* Java 6, 7 or 8 (Note that Oracle's JDK 6 library will fail to build Flink, but is able to run a pre-compiled package without problem)

```
git clone https://github.com/apache/incubator-flink.git
cd incubator-flink
mvn clean package -DskipTests # this will take up to 5 minutes
```

Flink is now installed in `flink-dist/target`


<<<<<<< HEAD
=======
## Developing Flink

The Flink committers use the IntelliJ IDE and Eclipse to develop the Flink codebase.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala


### IntelliJ

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [http://plugins.jetbrains.com/plugin/?id=1347](http://plugins.jetbrains.com/plugin/?id=1347)

### Eclipse Scala IDE

For Eclipse users, we recommend using Scala IDE 3.0.3, based on Eclipse Kepler. While this is a slightly older version,
we found it to be the verstion that works most robustly for a complex project like Flink.

Further details, and a guide to newer Scala IDE versions can be found in the
[How to setup Eclipse](https://github.com/apache/flink/blob/master/docs/internal_setup_eclipse.md) docs.

**Note:** Before following this setup, make sure to run the build from the command line once
(`mvn clean package -DskipTests`, see above)

1. Download the Scala IDE (preferred) or install the plugin to Eclipse Kepler. See 
   [How to setup Eclipse](https://github.com/apache/flink/blob/master/docs/internal_setup_eclipse.md) for download links and instructions.
2. Add the "macroparadise" compiler plugin to the Scala compiler.
   Open "Window" -> "Preferences" -> "Scala" -> "Compiler" -> "Advanced" and put into the "Xplugin" field the path to
   the *macroparadise* jar file (typically "/home/*-your-user-*/.m2/repository/org/scalamacros/paradise_2.10.4/2.0.1/paradise_2.10.4-2.0.1.jar").
   Note: If you do not have the jar file, you probably did not ran the command line build.
3. Import the Flink Maven projects ("File" -> "Import" -> "Maven" -> "Existing Maven Projects") 
4. During the import, Eclipse will ask to automatically install additional Maven build helper plugins.
5. Close the "flink-java8" project. Since Eclipse Kepler does not support Java 8, you cannot develop this project.

>>>>>>> 7f659f6... [docs] Update README and internals (scheduling) for graduation and fix broken links

## Support
Don’t hesitate to ask!

Please contact the developers on our [mailing lists](http://flink.apache.org/community.html#mailing-lists) if you need help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink.


## Documentation

<<<<<<< HEAD
The documentation of Apache Flink is located on the website: http://flink.incubator.apache.org or in the `docs/` directory of the source code.
=======
The documentation of Apache Flink is located on the website: [http://flink.apache.org](http://flink.apache.org)
or in the `docs/` directory of the source code.
>>>>>>> 7f659f6... [docs] Update README and internals (scheduling) for graduation and fix broken links


## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it. 
Contact us if you are looking for implementation tasks that fit your skills.
This article describes [how to contribute to Apache Flink](http://flink.apache.org/how-to-contribute.html).


## About

Apache Flink is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator PMC. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.

