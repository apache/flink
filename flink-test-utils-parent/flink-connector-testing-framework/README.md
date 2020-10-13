# Flink Connector Testing Framework
This module is dedicated to providing a unified and easy-to-use testing framework for all Flink-managed and third-party 
connectors in Flink eco-system. 

## Overview
The testing framework bases on JUnit for test case running and resource lifecycle management, and Testcontainers 
for setting up test environments. 

The design goal of the framework is: connector authors only need to provide 
instance of their source/sink and it's related external system, then the framework will deal with tests with bundled IT and
end-to-end test cases.

### Top-level design
A test consists of three parts:

#### Test scenario
Test scenario represents a specific workflow of running a test, such as testing basic functionalities, JM/TM failure handling, 
snapshotting etc. On code level, a scenario is a method in test suite. 

#### Test context
Test context describes how to interact with the external system, such as providing instance of source/sink related 
to the external system, and pattern of terminating Flink jobs using given source/sink. 

#### Test environment
Test environment provides an execution environment of running the Flink job for testing (mini cluster, local standalone cluster,
Flink on Docker...), also a configuration for configuring the environment and passing other required information into the test scenario.

In a nutshell, a test suite is represented as below on code level:

```java
public class TestSuite {
    public static void testBasicFuntionality(TestContext context, TestEnvironment env) {
        // Implementation for testing basic functionality scenario
    }
    
    public static void testTaskManagerFailure(TestContext context, TestEnvironment env) {
        // Implementation for testing TM failure scenario
    }
}
```

Different combination of scenarios, contexts and environments will construct different tests: 

```java
public class KafkaIT { 
    @Test
    public void basicTest() {
        // Prepare context and environment here
        TestSuite.testBasicFunctionality(kafkaTestContext, miniClusterTestEnvironment);
    }
    
    @Test
    public void failureTest() {
        // Prepare context and environment here
        TestSuite.testTaskManagerFailure(kafkaTestContext, miniClusterTestEnvironment);
    }
}

public class KafkaE2E {
    @Test
    public void basicTest() {
        // Prepare context and environment here
        TestSuite.testBasicFunctionality(kafkaTestContext, flinkContainersTestEnvironment);
    }
    
    @Test
    public void failureTest() {
        // Prepare context and environment here
        TestSuite.testTaskManagerFailure(kafkaTestContext, flinkContainersTestEnvironment);
    }
}
```

## Integrated components
The framework provides some integrated components as below: 

### Flink Test Environments
The framework will provide these environments:
- Mini-cluster test environment, which will run test locally with Flink mini-cluster, usually for integrated tests
- Flink containers test environment, which will run test on Flink on Testcontainers, usually for end-to-end tests
- Remote test environment, which can be assigned with arbitrary host/port of a remote Flink cluster, such as standalone, YARN etc.

### Flink TestContainers cluster
The framework provides a ```FlinkContainers``` and its 
related client for job managing and monitoring is provided for running end-to-end test cases. 

### Abstraction of external system
Communication with external system is indispensable for all connectors, thus we provide abstractions in order to adapt
to and manage most of external systems used in integrated and end-to-end tests: 

- ```ExternalSystem```, as the base of all external system. Users of the framework can extend this base class and 
implement their own external systems. This class extends ```ExternalResource``` class provided by JUnit so that the 
lifecycle of the external system could be controlled by JUnit. 
- ```ContainerizedExternalSystem```, specially for external systems based on Docker containers. The official repo of 
Testcontainers provides some out-of-box containers that you can use directly as your external system. Also if you want
to customize your own containerized external system, just extend this class and make your own implementation. 

### Test suites
The framework will provide a wide range of test cases and try to cover as many scenarios as possible. Currently, we 
provide a basic functionality test as an example, and more test scenarios will be added into the suite in the future. 

### Utilities
Some helper utilities are also provided in the framework for test data generating, validating etc.:
- File comparing for validating test data
- ```ControllableSource```: a source can be controlled by Java RMI remotely outside of the Flink cluster. This can be
used for generating random strings one/a batch at a time. 

## How to use the framework for testing my connector?
3-Easy-Steps-To-Construct-Your-Test:
1. Implement your own external system based on ```ExternalSystem``` or ```ContainerizedExternalSystem```
2. Implement your own test context related to your external system based on ```TestContext```
3. Invoke methods in test suite with your context and a suitable test environment

Also, you can always develop your own test suites and test environment by just extending or implementing related interfaces.

Have fun with your test!
