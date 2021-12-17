# Flink Connector Testing Framework

This module is dedicated to providing a unified and easy-to-use testing framework for all
Flink-managed and third-party connectors in Flink eco-system.

## Overview

The testing framework bases on JUnit 5 for test case running and resource lifecycle management. The
design goal of the framework is: connector developer only needs to provide instance of their
source/sink and it's related external system, then the framework will deal with tests with bundled
IT and end-to-end test cases.

## Interfaces

### Test Environment

Test environment provides an execution environment of running the Flink job for testing (mini
cluster, local standalone cluster, Flink on Docker...), also a configuration for configuring the
environment and passing other required information into the test scenario.

The framework provides MiniCluster and Flink on Docker out-of-box. You can also implement your own
test environment by implementing interface ```TestEnvironment```.

### External System & Context

You can define your own external system by implementing interface ```ExternalSystem```, and testing
framework will handle the lifecycle of external system automatically.

External context defines how to interact with the external system, providing instance of source and
connecting to the external system, test data and a writer for producing data into external system.

## Using Testing Framework in Your Connector

Testing framework using JUnit 5 features for supporting test cases running. We provide a base class
```SourceTestSuiteBase``` with some basic test cases. You can start simply by extending this base class,
instantiate test environment, external system and context in your test class constructor, and
annotate them with ```@WithTestEnvironment```, ```@WithExternalSystem```
and ```@WithExternalContextFactory```.

You can refer to ```KafkaSourceE2ECase``` as an example for using the testing framework.

Also, you can develop your own test cases in your test class. Just simply annotate your test cases
with ```@Case```, and testing framework will inject test environments and external systems into you
cases.
