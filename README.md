# Apache Flink (incubating)


Apache Flink is an open source system for expressive, declarative, fast, and efficient data analysis. Flink combines the scalability and programming flexibility of distributed MapReduce-like platforms with the efficiency, out-of-core execution, and query optimization capabilities found in parallel databases.


Learn more about Flink at http://flink.incubator.apache.org/


## Build Apache Flink

###  Build From Source


#### Requirements
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



## Support
Don’t hesitate to ask!

Please contact the developers on our [mailing lists](http://flink.incubator.apache.org/community.html#mailing-lists) if you need help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink.


## Documentation

The documentation of Apache Flink is located on the website: http://flink.incubator.apache.org or in the `docs/` directory of the source code.


## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it. 
Contact us if you are looking for implementation tasks that fit your skills.
This article describes [how to contribute to Apache Flink](http://flink.incubator.apache.org/how-to-contribute.html).


## About

Apache Flink is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator PMC. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.
