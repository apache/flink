# Apache Flink Overview

<details>
<summary>Relevant source files</summary>

The following files were used as context for generating this wiki page:

- [docs/content.zh/docs/dev/python/datastream_tutorial.md](docs/content.zh/docs/dev/python/datastream_tutorial.md)
- [docs/content.zh/docs/dev/python/installation.md](docs/content.zh/docs/dev/python/installation.md)
- [docs/content.zh/docs/dev/python/table_api_tutorial.md](docs/content.zh/docs/dev/python/table_api_tutorial.md)
- [docs/content.zh/docs/flinkDev/building.md](docs/content.zh/docs/flinkDev/building.md)
- [docs/content.zh/docs/try-flink/local_installation.md](docs/content.zh/docs/try-flink/local_installation.md)
- [docs/content/docs/dev/python/datastream_tutorial.md](docs/content/docs/dev/python/datastream_tutorial.md)
- [docs/content/docs/dev/python/installation.md](docs/content/docs/dev/python/installation.md)
- [docs/content/docs/dev/python/table_api_tutorial.md](docs/content/docs/dev/python/table_api_tutorial.md)
- [docs/content/docs/flinkDev/building.md](docs/content/docs/flinkDev/building.md)
- [docs/content/docs/try-flink/local_installation.md](docs/content/docs/try-flink/local_installation.md)
- [flink-clients/src/main/java/org/apache/flink/client/deployment/application/ApplicationDispatcherGatewayServiceFactory.java](flink-clients/src/main/java/org/apache/flink/client/deployment/application/ApplicationDispatcherGatewayServiceFactory.java)
- [flink-dist/src/main/resources/META-INF/NOTICE](flink-dist/src/main/resources/META-INF/NOTICE)
- [flink-end-to-end-tests/test-scripts/test_kubernetes_application.sh](flink-end-to-end-tests/test-scripts/test_kubernetes_application.sh)
- [flink-filesystems/flink-azure-fs-hadoop/src/main/resources/META-INF/NOTICE](flink-filesystems/flink-azure-fs-hadoop/src/main/resources/META-INF/NOTICE)
- [flink-filesystems/flink-fs-hadoop-shaded/src/main/resources/META-INF/NOTICE](flink-filesystems/flink-fs-hadoop-shaded/src/main/resources/META-INF/NOTICE)
- [flink-filesystems/flink-gs-fs-hadoop/src/main/resources/META-INF/NOTICE](flink-filesystems/flink-gs-fs-hadoop/src/main/resources/META-INF/NOTICE)
- [flink-filesystems/flink-s3-fs-hadoop/src/main/resources/META-INF/NOTICE](flink-filesystems/flink-s3-fs-hadoop/src/main/resources/META-INF/NOTICE)
- [flink-filesystems/flink-s3-fs-presto/src/main/resources/META-INF/NOTICE](flink-filesystems/flink-s3-fs-presto/src/main/resources/META-INF/NOTICE)
- [flink-formats/flink-sql-avro-confluent-registry/src/main/resources/META-INF/NOTICE](flink-formats/flink-sql-avro-confluent-registry/src/main/resources/META-INF/NOTICE)
- [flink-formats/flink-sql-avro/src/main/resources/META-INF/NOTICE](flink-formats/flink-sql-avro/src/main/resources/META-INF/NOTICE)
- [flink-kubernetes/pom.xml](flink-kubernetes/pom.xml)
- [flink-kubernetes/src/main/resources/META-INF/NOTICE](flink-kubernetes/src/main/resources/META-INF/NOTICE)
- [flink-kubernetes/src/test/java/org/apache/flink/kubernetes/highavailability/KubernetesLeaderRetrievalDriverTest.java](flink-kubernetes/src/test/java/org/apache/flink/kubernetes/highavailability/KubernetesLeaderRetrievalDriverTest.java)
- [flink-python/README.md](flink-python/README.md)
- [flink-python/apache-flink-libraries/setup.py](flink-python/apache-flink-libraries/setup.py)
- [flink-python/dev/build-wheels.sh](flink-python/dev/build-wheels.sh)
- [flink-python/dev/dev-requirements.txt](flink-python/dev/dev-requirements.txt)
- [flink-python/dev/lint-python.sh](flink-python/dev/lint-python.sh)
- [flink-python/pom.xml](flink-python/pom.xml)
- [flink-python/pyflink/fn_execution/formats/avro.py](flink-python/pyflink/fn_execution/formats/avro.py)
- [flink-python/pyflink/gen_protos.py](flink-python/pyflink/gen_protos.py)
- [flink-python/pyproject.toml](flink-python/pyproject.toml)
- [flink-python/setup.py](flink-python/setup.py)
- [flink-python/src/main/resources/META-INF/NOTICE](flink-python/src/main/resources/META-INF/NOTICE)
- [flink-python/tox.ini](flink-python/tox.ini)
- [flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/Dispatcher.java](flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/Dispatcher.java)
- [flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/DispatcherFactory.java](flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/DispatcherFactory.java)
- [flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/DispatcherServices.java](flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/DispatcherServices.java)
- [flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/PartialDispatcherServices.java](flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/PartialDispatcherServices.java)
- [flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/PartialDispatcherServicesWithJobPersistenceComponents.java](flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/PartialDispatcherServicesWithJobPersistenceComponents.java)
- [flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/SessionDispatcherFactory.java](flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/SessionDispatcherFactory.java)
- [flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/StandaloneDispatcher.java](flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/StandaloneDispatcher.java)
- [flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/runner/AbstractDispatcherLeaderProcess.java](flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/runner/AbstractDispatcherLeaderProcess.java)
- [flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/runner/DefaultDispatcherGatewayServiceFactory.java](flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/runner/DefaultDispatcherGatewayServiceFactory.java)
- [flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/JobResult.java](flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/JobResult.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/checkpoint/ZKCheckpointIDCounterMultiServersTest.java](flink-runtime/src/test/java/org/apache/flink/runtime/checkpoint/ZKCheckpointIDCounterMultiServersTest.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/checkpoint/ZooKeeperCompletedCheckpointStoreTest.java](flink-runtime/src/test/java/org/apache/flink/runtime/checkpoint/ZooKeeperCompletedCheckpointStoreTest.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/AbstractDispatcherTest.java](flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/AbstractDispatcherTest.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/DispatcherCleanupITCase.java](flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/DispatcherCleanupITCase.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/DispatcherResourceCleanupTest.java](flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/DispatcherResourceCleanupTest.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/DispatcherTest.java](flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/DispatcherTest.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/TestingDispatcher.java](flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/TestingDispatcher.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/TestingJobManagerRunnerFactory.java](flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/TestingJobManagerRunnerFactory.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/TestingPartialDispatcherServices.java](flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/TestingPartialDispatcherServices.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/runner/DefaultDispatcherRunnerITCase.java](flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/runner/DefaultDispatcherRunnerITCase.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/runner/ZooKeeperDefaultDispatcherRunnerTest.java](flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/runner/ZooKeeperDefaultDispatcherRunnerTest.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/jobmaster/TestUtils.java](flink-runtime/src/test/java/org/apache/flink/runtime/jobmaster/TestUtils.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/jobmaster/TestingJobManagerRunner.java](flink-runtime/src/test/java/org/apache/flink/runtime/jobmaster/TestingJobManagerRunner.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/leaderelection/LeaderChangeClusterComponentsTest.java](flink-runtime/src/test/java/org/apache/flink/runtime/leaderelection/LeaderChangeClusterComponentsTest.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/leaderelection/StandaloneLeaderElectionTest.java](flink-runtime/src/test/java/org/apache/flink/runtime/leaderelection/StandaloneLeaderElectionTest.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/leaderelection/TestingRetrievalBase.java](flink-runtime/src/test/java/org/apache/flink/runtime/leaderelection/TestingRetrievalBase.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/leaderretrieval/DefaultLeaderRetrievalServiceTest.java](flink-runtime/src/test/java/org/apache/flink/runtime/leaderretrieval/DefaultLeaderRetrievalServiceTest.java)
- [flink-runtime/src/test/java/org/apache/flink/runtime/leaderretrieval/SettableLeaderRetrievalServiceTest.java](flink-runtime/src/test/java/org/apache/flink/runtime/leaderretrieval/SettableLeaderRetrievalServiceTest.java)
- [flink-state-backends/flink-statebackend-forst/pom.xml](flink-state-backends/flink-statebackend-forst/pom.xml)
- [flink-state-backends/flink-statebackend-forst/src/test/resources/log4j2-test.properties](flink-state-backends/flink-statebackend-forst/src/test/resources/log4j2-test.properties)
- [flink-state-backends/pom.xml](flink-state-backends/pom.xml)
- [pom.xml](pom.xml)
- [tools/azure-pipelines/build-python-wheels.yml](tools/azure-pipelines/build-python-wheels.yml)
- [tools/releasing/create_binary_release.sh](tools/releasing/create_binary_release.sh)

</details>



## Purpose and Scope

This document provides a comprehensive overview of Apache Flink as a distributed stream processing framework, covering its core architecture, key components, and programming interfaces. It introduces the fundamental concepts and system design that enable Flink to process both bounded and unbounded data streams with low latency and high throughput.

This overview focuses on the overall system architecture and major components. For detailed information about specific subsystems, see: Core Runtime Architecture ([2](#2)), Programming APIs ([3](#3)), System Management & Monitoring ([4](#4)), Connectors & External Integrations ([5](#5)), and Development & Build Infrastructure ([6](#6)).

## What is Apache Flink

Apache Flink is a distributed stream processing framework designed for stateful computations over unbounded and bounded data streams. It provides unified APIs for both batch and stream processing, enabling developers to build scalable data processing applications with exactly-once semantics and low-latency processing capabilities.

The framework operates on the principle of treating batch processing as a special case of stream processing, where bounded datasets are processed using the same runtime and APIs as unbounded streams. This approach provides consistency across different workload types while maintaining high performance.

Sources: [pom.xml:33-36](), [flink-dist/src/main/resources/META-INF/NOTICE:1-6]()

## Overall System Architecture

Flink's architecture consists of several interconnected layers that work together to provide a complete data processing platform:

```mermaid
graph TB
    subgraph "User APIs Layer"
        DSA["DataStream API<br/>(flink-streaming-java)"]
        TA["Table API<br/>(flink-table)"]
        SQL["SQL Interface<br/>(flink-table)"]
        PY["PyFlink<br/>(flink-python)"]
    end
    
    subgraph "Core Runtime Layer"
        DISP["Dispatcher<br/>(org.apache.flink.runtime.dispatcher)"]
        JM["JobManager<br/>(flink-runtime)"]
        TM["TaskManager<br/>(flink-runtime)"]
        AS["AdaptiveScheduler<br/>(flink-runtime)"]
    end
    
    subgraph "State Management Layer"
        SB["State Backends<br/>(flink-state-backends)"]
        CP["CheckpointCoordinator<br/>(flink-runtime)"]
        FORST["ForSt Backend<br/>(flink-statebackend-forst)"]
        ROCKS["RocksDB Backend<br/>(flink-statebackend-rocksdb)"]
    end
    
    subgraph "Infrastructure Layer"
        FS["File Systems<br/>(flink-filesystems)"]
        CONN["Connectors<br/>(flink-connectors)"]
        METRICS["Metrics System<br/>(flink-metrics)"]
        K8S["Kubernetes Integration<br/>(flink-kubernetes)"]
    end
    
    subgraph "Build System"
        MAVEN["Maven Build<br/>(pom.xml)"]
        CI["CI/CD Pipeline<br/>(tools/azure-pipelines)"]
    end
    
    DSA --> JM
    TA --> JM
    SQL --> TA
    PY --> DSA
    PY --> TA
    
    DISP --> JM
    JM --> TM
    JM --> AS
    
    TM --> SB
    SB --> FORST
    SB --> ROCKS
    JM --> CP
    
    JM --> FS
    JM --> CONN
    TM --> METRICS
    JM --> K8S
    
    MAVEN --> CI
```

This architecture provides a layered approach where each layer has specific responsibilities:
- **User APIs Layer**: Provides different programming interfaces for various use cases
- **Core Runtime Layer**: Manages job execution, scheduling, and cluster coordination
- **State Management Layer**: Handles fault tolerance and stateful computations
- **Infrastructure Layer**: Provides connectivity and deployment capabilities

Sources: [pom.xml:71-109](), [flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/Dispatcher.java:137-142]()

## Core Runtime Components

The core runtime forms the heart of Flink's execution engine, consisting of several key components that coordinate job execution:

```mermaid
graph TB
    subgraph "Job Submission Flow"
        CLIENT["Client Application"]
        DISP["Dispatcher<br/>(org.apache.flink.runtime.dispatcher.Dispatcher)"]
        JMR["JobManagerRunner<br/>(org.apache.flink.runtime.jobmaster.JobManagerRunner)"]
        JM["JobManager<br/>(org.apache.flink.runtime.jobmanager)"]
    end
    
    subgraph "Scheduling Components"
        AS["AdaptiveScheduler<br/>(org.apache.flink.runtime.scheduler.adaptive)"]
        RM["ResourceManager<br/>(org.apache.flink.runtime.resourcemanager)"]
        SLOTS["SlotPool<br/>(org.apache.flink.runtime.jobmaster.slotpool)"]
    end
    
    subgraph "Task Execution"
        TM["TaskManager<br/>(org.apache.flink.runtime.taskmanager)"]
        ST["StreamTask<br/>(org.apache.flink.streaming.runtime.tasks)"]
        OP["Operators<br/>(org.apache.flink.streaming.api.operators)"]
    end
    
    subgraph "Fault Tolerance"
        CC["CheckpointCoordinator<br/>(org.apache.flink.runtime.checkpoint)"]
        SP["Savepoint Management"]
        HA["High Availability<br/>(org.apache.flink.runtime.highavailability)"]
    end
    
    CLIENT --> DISP
    DISP --> JMR
    JMR --> JM
    
    JM --> AS
    AS --> RM
    RM --> SLOTS
    SLOTS --> TM
    
    TM --> ST
    ST --> OP
    
    JM --> CC
    CC --> SP
    JM --> HA
```

The `Dispatcher` serves as the entry point for job submissions and manages the lifecycle of jobs within the cluster. It coordinates with the `JobManagerRunner` to spawn individual job execution instances and handles resource cleanup when jobs terminate.

Sources: [flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/Dispatcher.java:136-140](), [flink-runtime/src/test/java/org/apache/flink/runtime/dispatcher/DispatcherTest.java:153-163]()

## Programming APIs and Execution Plans

Flink provides multiple programming APIs that cater to different use cases and levels of abstraction:

```mermaid
graph TB
    subgraph "API Hierarchy"
        SQL_API["SQL API<br/>(flink-table)"]
        TABLE_API["Table API<br/>(flink-table)"]
        DS_API["DataStream API<br/>(flink-streaming-java)"]
        PROCESS["Process Functions<br/>(Low-level API)"]
    end
    
    subgraph "Python Support"
        PYFLINK["PyFlink<br/>(flink-python)"]
        PY4J["Py4J Bridge<br/>(py4j)"]
        BEAM["Apache Beam<br/>(beam-runners-java-fn-execution)"]
    end
    
    subgraph "Execution Plan Generation"
        EG["ExecutionGraph<br/>(flink-runtime)"]
        JG["JobGraph<br/>(flink-runtime)"]
        SG["StreamGraph<br/>(flink-streaming-java)"]
    end
    
    SQL_API --> TABLE_API
    TABLE_API --> DS_API
    DS_API --> PROCESS
    
    PYFLINK --> PY4J
    PY4J --> TABLE_API
    PY4J --> DS_API
    PYFLINK --> BEAM
    
    DS_API --> SG
    SG --> JG
    JG --> EG
```

The Python API (`PyFlink`) provides a bridge to Flink's Java runtime through the `Py4J` library, enabling Python developers to leverage Flink's capabilities while maintaining the performance characteristics of the underlying Java execution engine.

Sources: [flink-python/pom.xml:32-34](), [flink-python/setup.py:319-330](), [flink-python/dev/dev-requirements.txt:18-36]()

## State Management Architecture

Flink's state management system provides fault tolerance and enables stateful stream processing through a sophisticated checkpointing mechanism:

```mermaid
graph TB
    subgraph "State Backends"
        MEM["Memory State Backend<br/>(org.apache.flink.runtime.state.memory)"]
        FS_BACKEND["File System State Backend<br/>(org.apache.flink.runtime.state.filesystem)"]
        ROCKS["RocksDB State Backend<br/>(flink-statebackend-rocksdb)"]
        FORST["ForSt State Backend<br/>(flink-statebackend-forst)"]
    end
    
    subgraph "Checkpoint System"
        CC["CheckpointCoordinator<br/>(org.apache.flink.runtime.checkpoint.CheckpointCoordinator)"]
        PEND["PendingCheckpoint<br/>(org.apache.flink.runtime.checkpoint)"]
        COMP["CompletedCheckpoint<br/>(org.apache.flink.runtime.checkpoint)"]
        STORE["CheckpointStore<br/>(org.apache.flink.runtime.checkpoint)"]
    end
    
    subgraph "State Types"
        KEYED["Keyed State<br/>(org.apache.flink.api.common.state)"]
        OP_STATE["Operator State<br/>(org.apache.flink.api.common.state)"]
        BROADCAST["Broadcast State<br/>(org.apache.flink.api.common.state)"]
    end
    
    subgraph "Storage Layer"
        HDFS["HDFS/S3<br/>(flink-filesystems)"]
        LOCAL["Local Storage"]
        CACHE["File Cache<br/>(org.apache.flink.runtime.state.filesystem)"]
    end
    
    CC --> PEND
    PEND --> COMP
    COMP --> STORE
    
    KEYED --> ROCKS
    KEYED --> FORST
    OP_STATE --> FS_BACKEND
    BROADCAST --> MEM
    
    ROCKS --> LOCAL
    FORST --> HDFS
    FORST --> CACHE
    
    STORE --> HDFS
```

The ForSt state backend, implemented in `flink-statebackend-forst`, provides high-performance state storage with support for incremental checkpoints and efficient state access patterns.

Sources: [flink-state-backends/flink-statebackend-forst/pom.xml:33-67](), [flink-dist/src/main/resources/META-INF/NOTICE:10-11]()

## Infrastructure and Deployment

Flink's infrastructure layer supports various deployment environments and external integrations:

```mermaid
graph TB
    subgraph "Deployment Targets"
        K8S["Kubernetes<br/>(flink-kubernetes)"]
        YARN["YARN<br/>(flink-yarn)"]
        STANDALONE["Standalone Cluster"]
    end
    
    subgraph "File Systems"
        S3["S3 File System<br/>(flink-s3-fs-hadoop,<br/>flink-s3-fs-presto)"]
        AZURE["Azure File System<br/>(flink-azure-fs-hadoop)"]
        GS["Google Cloud Storage<br/>(flink-gs-fs-hadoop)"]
        HDFS["HDFS<br/>(flink-fs-hadoop-shaded)"]
    end
    
    subgraph "Connectors"
        KAFKA["Kafka Connector<br/>(flink-sql-connector-kafka)"]
        JDBC["JDBC Connector<br/>(flink-connector-jdbc)"]
        ELASTIC["Elasticsearch Connector<br/>(flink-sql-connector-elasticsearch7)"]
        HIVE["Hive Connector<br/>(flink-connector-hive)"]
    end
    
    subgraph "Build System"
        MAVEN["Maven Build<br/>(pom.xml)"]
        AZURE_CI["Azure Pipelines<br/>(tools/azure-pipelines)"]
        PYTHON_WHEELS["Python Wheel Build<br/>(flink-python/dev/build-wheels.sh)"]
    end
    
    K8S --> S3
    K8S --> AZURE
    YARN --> HDFS
    
    S3 --> KAFKA
    HDFS --> HIVE
    AZURE --> JDBC
    
    MAVEN --> AZURE_CI
    AZURE_CI --> PYTHON_WHEELS
```

The Kubernetes integration, provided by the `flink-kubernetes` module, enables native deployment of Flink clusters on Kubernetes with support for dynamic resource allocation and container orchestration.

Sources: [flink-kubernetes/pom.xml:29-31](), [flink-kubernetes/src/main/resources/META-INF/NOTICE:1-5](), [tools/azure-pipelines/build-python-wheels.yml:1-2]()

## Build System and Development Workflow

Flink uses a comprehensive Maven-based build system that supports multiple programming languages and deployment targets:

```mermaid
graph LR
    subgraph "Source Code"
        JAVA["Java Modules<br/>(flink-*)"]
        PYTHON["Python Code<br/>(flink-python)"]
        SCALA["Scala Code<br/>(flink-scala)"]
    end
    
    subgraph "Build Process"
        MAVEN["Maven Build<br/>(pom.xml)"]
        COMPILE["Compilation"]
        TEST["Testing"]
        PACKAGE["Packaging"]
    end
    
    subgraph "Python Packaging"
        SETUP_PY["setup.py<br/>(flink-python/setup.py)"]
        WHEEL["Wheel Generation"]
        CYTHON["Cython Extensions<br/>(*.pyx files)"]
    end
    
    subgraph "Artifacts"
        JAR["JAR Files"]
        PYTHON_PKG["Python Packages<br/>(apache-flink)"]
        DIST["Distribution<br/>(flink-dist)"]
    end
    
    JAVA --> MAVEN
    PYTHON --> SETUP_PY
    SCALA --> MAVEN
    
    MAVEN --> COMPILE
    COMPILE --> TEST
    TEST --> PACKAGE
    
    SETUP_PY --> CYTHON
    CYTHON --> WHEEL
    
    PACKAGE --> JAR
    WHEEL --> PYTHON_PKG
    JAR --> DIST
```

The build system supports both traditional Java/Scala components and Python extensions, with specialized handling for PyFlink's native extensions and dependency management.

Sources: [pom.xml:18-31](), [flink-python/setup.py:102-171](), [flink-python/pyproject.toml:18-37](), [flink-python/dev/build-wheels.sh:1-10]()
