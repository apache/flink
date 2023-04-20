# flink-architecture-tests-test

This submodule contains rules defined centrally for test code architectural tests that will be
developed in each submodule individually where the test code has been developed.

## Why the architecture test infra for production code and for test code are different

Compared to `flink-architecture-tests-production` which implements and executes architectural rules
centrally, `flink-architecture-tests-test` implements the rules centrally, but they are executed
individually in each submodule. This is done for the following reasons:

- Reduce the classpath complexity. In order to execute the tests centrally, each submodule would
  have to provide a test-jar. However, this does not work well in IntelliJ which doesn't support all
  Maven configurations (such as exclusion filters); this would cause the rules to behave differently
  when executed from Maven versus IntelliJ. Furthermore, creating more test-jars is not desirable
  and increases the project complexity.
- Separation of concerns. Test code should be viewed as internal to a module, and by separating the
  violation stores for each submodule this separation can be maintained. Unlike the production code
  tests there is few shared code between test code, and thus the performance benefits do not apply.
- Flexibility. Each submodule can not only import the generic rules defined
  in `flink-architecture-tests-test` centrally, but also develop further module-specific
  architectural tests locally.

## How do I write a new architectural rule?

Please refer to [README](../README.md).

## How do I initialize and develop the first architectural test for a Flink submodule's test code?

If there already exists any architectural test in the Flink submodule, you can skip this section and
start writing your architectural tests and rules.

It is recommended to stick to the following template to setup and develop the first test code
architectural test for a Flink submodule.

Under a Flink submodule where no architectural test has ever been developed.

- Create `archunit.properties` and `flink-connector-file` under "<Flink Submodule>
  /src/test/resources". Please use [archunit.properties](./src/test/resources/archunit.properties)
  and [log4j2-test.properties](./src/test/resources/log4j2-test.properties) as template.
- Develop ArchUnit test under the package `org.apache.flink.architecture`. It is recommended to
  use `TestCodeArchitectureTest` as the class name.
- Include the common tests
  via `@ArchTest public static final ArchTests COMMON_TESTS = ArchTests.in(TestCodeArchitectureTestBase.class)`
  .
- Develop individual rules under the package `org.apache.flink.architecture.rules` if it is
  required.

For practice purpose, please refer to the `flink-connector-file` as the reference implementation.

## What should I do if the tests fail?

Please refer to [README](../README.md).

## How do I test Scala classes?

Please refer to [README](../README.md).

## What should I do if I modify an existing rule?

In that case, you need to regenerate all the stores. Just run this on the root of the project:

```
rm -rf `find . -type d -name archunit-violations`
mvn test -Dtest="*TestCodeArchitectureTest*" -DfailIfNoTests=false -Darchunit.freeze.refreeze=true -Darchunit.freeze.store.default.allowStoreCreation=true -Dfast
```
