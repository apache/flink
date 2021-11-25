# flink-architecture-tests

This module contains architecture tests using [ArchUnit](https://www.archunit.org/). These tests 
reside in their own module in order to control the classpath of which modules should be tested.
Running these tests together (rather than individually per module) allows caching the imported
classes for better performance.

## What should I do if the tests fail?

There are two cases in which the architectural tests may fail due to changes in the codebase:
1. You have resolved an existing violation.
2. Your change introduced a new violation.

In the former case, please add the updated violation store file to your commit. The tests should now
succeed.

If you have added a new violation, consider how to proceed. New violations should be avoided at all
costs. First and foremost, evaluate whether the flagged violation is correct. If it is, try to rework
your change to avoid the violation in the first place. However, if you believe that your code should
not be flagged as a violation, open a JIRA issue so that the rule can be improved.

In order to have a new violation recorded, open `archunit.properties` and enable
`freeze.refreeze=true`. Rerun the tests and revert the change to the configuration. The new
violation should now have been added to the existing store.

## How do I add a module?

In order to add a module to be tested against, add it as a test dependency in this module's 
`pom.xml`.

## How do I write a new architectural rule?

Please refer to the [ArchUnit user guide](https://www.archunit.org/) for general documentation.
However, there are a few points to consider when writing rules:

1. If there are existing violations which cannot be fixed right away, the rule must be _frozen_ by
   wrapping it in `FreezingArchRule.freeze()`. This will add the rule to the violation store that 
   records the existing violations. Add the new stored violations file within `violations` to your 
   commit.
2. ArchUnit does not work well with Scala classes. All rules should exclude non-Java classes by
   utilizing the methods in `GivenJavaClasses`.

## How do I test Scala classes?

Scala is not supported by ArchUnit. Although it operates on the byte-code level and can, in general,
process classes compiled from Scala as well, there are Scala-specific constructs that do not work
well in practice. Therefore, all architecture rules should exclude non-Java classes.
