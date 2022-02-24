# flink-architecture-tests

This module contains architecture tests using [ArchUnit](https://www.archunit.org/). Considering the
isolation of classpath and rules for architectural tests, there are two top level categories:

- production code architectural tests
- test code architectural tests

Since both of them will need some common ArchUnit extensions, there are three submodules:

- flink-architecture-tests-base - contains common ArchUnit extensions that will be used for both
  production code architectural tests and test code architectural tests.
- flink-architecture-tests-production - contains all architectural rules and tests centrally for
  production code. Please read the [README](flink-architecture-tests-production/README.md) of
  flink-architecture-tests-production for further information.
- flink-architecture-tests - contains architectural rules centrally for test code. The architectural
  test will be built individually in each submodule where the test code has been developed. Please
  read the [README](flink-architecture-tests-test/README.md) of flink-architecture-tests-test for
  further information.

Following documentation is valid for building and maintaining the architectural tests both for the
production code and the test code.

## What should I do if the tests fail?

There are two cases in which architectural tests may fail due to changes in the codebase:

1. You have resolved an existing violation.
2. Your change introduced a new violation.

In the former case, please add the updated violation store file to your commit. The tests should now
succeed.

If you have added a new violation, consider how to proceed. New violations should be avoided at all
costs. First and foremost, evaluate whether the flagged violation is correct. If it is, try to
rework your change to avoid the violation in the first place. However, if you believe that your code
should not be flagged as a violation, open a JIRA issue so that the rule can be improved.

In order to have a new violation recorded, open `archunit.properties` in the submodule and enable
`freeze.refreeze=true`. Rerun the tests and revert the change to the configuration. The new
violation should now have been added to the existing store.

## How do I write a new architectural rule?

Please refer to the [ArchUnit user guide](https://www.archunit.org/) for general documentation.
However, there are a few points to consider when writing rules:

1. If there are existing violations which cannot be fixed right away, the rule must be _frozen_ by
   wrapping it in `FreezingArchRule.freeze()`. This will add the rule to the violation store that
   records the existing violations. Add the new stored violations file within `violations` to your
   commit.
2. Please give the freezing rule a fixed description by
   calling `FreezingArchRule.freeze().as(String newDescription)`. This will reduce the maintenance
   effort for each rule update. Otherwise, since the description will be used as the key of the rule
   to define the violation store, new violation stores will be created and old obsolete stores have
   to be removed manually each time when rules have been changed.
3. In order to allow creating new violation store file, open `archunit.properties` in the submodule
   and enable `freeze.store.default.allowStoreCreation=true`.
4. ArchUnit does not work well with Scala classes. All rules should exclude non-Java classes by
   utilizing the methods in `GivenJavaClasses`.

## How do I test Scala classes?

Scala is not supported by ArchUnit. Although it operates on the byte-code level and can, in general,
process classes compiled from Scala as well, there are Scala-specific constructs that do not work
well in practice. Therefore, all architecture rules should exclude non-Java classes.
