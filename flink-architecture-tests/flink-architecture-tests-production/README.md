# flink-architecture-tests-production

This submodule contains all architectural tests of production code. 
Running these tests together (rather than individually per module) allows caching the imported
classes for better performance.

## What should I do if the tests fail?

Please refer to [README](../README.md).

## How do I write a new architectural rule?

Please refer to [README](../README.md).

## How do I write a ArchUnit test?

Please refer to the [ArchUnit user guide](https://www.archunit.org/) for general documentation. For
quick start, you could find an example
at [flink-architecture-tests-production/ArchitectureTest](./src/test/java/org/apache/flink/architecture/ArchitectureTest.java):

```java
@ArchTest
public static final ArchTests API_ANNOTATIONS=ArchTests.in(ApiAnnotationRules.class);
```

## How do I add a module?

In order to add a module to be tested against, add it as a test dependency in this module's
`pom.xml`.

## How do I test Scala classes?

Please refer to [README](../README.md).
