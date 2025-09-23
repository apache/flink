# CI/CD & Documentation Pipeline

<details>
<summary>Relevant source files</summary>

The following files were used as context for generating this wiki page:

- [.github/workflows/docs.sh](.github/workflows/docs.sh)
- [.github/workflows/docs.yml](.github/workflows/docs.yml)
- [.gitignore](.gitignore)
- [.idea/icon.png](.idea/icon.png)
- [.mvn/wrapper/maven-wrapper.properties](.mvn/wrapper/maven-wrapper.properties)
- [azure-pipelines.yml](azure-pipelines.yml)
- [docs/README.md](docs/README.md)
- [docs/build_docs.sh](docs/build_docs.sh)
- [docs/config.toml](docs/config.toml)
- [docs/content.zh/release-notes/flink-1.8.md](docs/content.zh/release-notes/flink-1.8.md)
- [docs/content.zh/release-notes/flink-1.9.md](docs/content.zh/release-notes/flink-1.9.md)
- [docs/content/release-notes/flink-1.8.md](docs/content/release-notes/flink-1.8.md)
- [docs/content/release-notes/flink-1.9.md](docs/content/release-notes/flink-1.9.md)
- [docs/setup_hugo.sh](docs/setup_hugo.sh)
- [docs/themes/.gitignore](docs/themes/.gitignore)
- [flink-architecture-tests/flink-architecture-tests-production/pom.xml](flink-architecture-tests/flink-architecture-tests-production/pom.xml)
- [flink-architecture-tests/pom.xml](flink-architecture-tests/pom.xml)
- [flink-end-to-end-tests/test-scripts/common_kubernetes.sh](flink-end-to-end-tests/test-scripts/common_kubernetes.sh)
- [flink-end-to-end-tests/test-scripts/container-scripts/kubernetes-pod-template.yaml](flink-end-to-end-tests/test-scripts/container-scripts/kubernetes-pod-template.yaml)
- [flink-end-to-end-tests/test-scripts/test_kubernetes_application_ha.sh](flink-end-to-end-tests/test-scripts/test_kubernetes_application_ha.sh)
- [flink-yarn/pom.xml](flink-yarn/pom.xml)
- [mvnw](mvnw)
- [mvnw.cmd](mvnw.cmd)
- [tools/azure-pipelines/build-apache-repo.yml](tools/azure-pipelines/build-apache-repo.yml)
- [tools/azure-pipelines/build-nightly-dist.yml](tools/azure-pipelines/build-nightly-dist.yml)
- [tools/azure-pipelines/e2e-template.yml](tools/azure-pipelines/e2e-template.yml)
- [tools/azure-pipelines/jobs-template.yml](tools/azure-pipelines/jobs-template.yml)
- [tools/ci/alibaba-mirror-settings.xml](tools/ci/alibaba-mirror-settings.xml)
- [tools/ci/compile.sh](tools/ci/compile.sh)
- [tools/ci/compile_ci.sh](tools/ci/compile_ci.sh)
- [tools/ci/docs.sh](tools/ci/docs.sh)
- [tools/ci/google-mirror-settings.xml](tools/ci/google-mirror-settings.xml)
- [tools/ci/license_check.sh](tools/ci/license_check.sh)
- [tools/ci/maven-utils.sh](tools/ci/maven-utils.sh)
- [tools/ci/shade.sh](tools/ci/shade.sh)
- [tools/ci/stage.sh](tools/ci/stage.sh)
- [tools/ci/ubuntu-mirror-list.txt](tools/ci/ubuntu-mirror-list.txt)
- [tools/ci/verify_bundled_optional.sh](tools/ci/verify_bundled_optional.sh)
- [tools/ci/verify_scala_suffixes.sh](tools/ci/verify_scala_suffixes.sh)

</details>



This document describes the continuous integration, continuous deployment, and documentation generation infrastructure for Apache Flink. It covers the automated build, test, and deployment processes that ensure code quality and maintain up-to-date documentation.

For information about the build system and dependency management, see [Build System & Dependencies](#6.1). For details about testing infrastructure, see [Testing Infrastructure](#6.2).

## Pipeline Architecture Overview

Flink's CI/CD infrastructure consists of multiple interconnected systems that handle different aspects of the development lifecycle. The primary components include GitHub Actions for documentation builds, Azure Pipelines for comprehensive testing and releases, and a Maven-based build system with extensive quality checks.

```mermaid
graph TB
    subgraph "GitHub Actions"
        GH_DOCS["docs.yml Workflow"]
        GH_DOCS_SCRIPT["docs.sh Script"]
    end
    
    subgraph "Azure Pipelines"
        AZ_MAIN["azure-pipelines.yml"]
        AZ_APACHE["build-apache-repo.yml"]
        AZ_JOBS["jobs-template.yml"]
        AZ_E2E["e2e-template.yml"]
        AZ_NIGHTLY["build-nightly-dist.yml"]
    end
    
    subgraph "Build Infrastructure"
        CI_COMPILE["compile.sh"]
        CI_MAVEN["maven-utils.sh"]
        CI_STAGE["stage.sh"]
        CI_SHADE["shade.sh"]
    end
    
    subgraph "Documentation System"
        DOCS_CONFIG["config.toml"]
        DOCS_SETUP["setup_hugo.sh"]
        DOCS_BUILD["build_docs.sh"]
        HUGO["Hugo Static Site Generator"]
    end
    
    GH_DOCS --> GH_DOCS_SCRIPT
    GH_DOCS_SCRIPT --> DOCS_SETUP
    GH_DOCS_SCRIPT --> HUGO
    
    AZ_MAIN --> AZ_JOBS
    AZ_APACHE --> AZ_JOBS
    AZ_JOBS --> AZ_E2E
    AZ_APACHE --> AZ_NIGHTLY
    
    AZ_JOBS --> CI_COMPILE
    CI_COMPILE --> CI_MAVEN
    CI_COMPILE --> CI_STAGE
    CI_COMPILE --> CI_SHADE
    
    DOCS_CONFIG --> HUGO
    DOCS_BUILD --> HUGO
```

Sources: [.github/workflows/docs.yml:1-75](), [azure-pipelines.yml:1-101](), [tools/azure-pipelines/build-apache-repo.yml:1-179](), [tools/ci/compile.sh:1-121](), [docs/config.toml:1-134]()

## GitHub Actions Workflows

### Documentation Pipeline

The documentation pipeline automatically builds and deploys Flink's documentation using GitHub Actions. The primary workflow is defined in `docs.yml` and runs on a nightly schedule.

```mermaid
flowchart LR
    TRIGGER["Nightly Schedule or Manual Trigger"]
    CHECKOUT["Checkout Code"]
    SET_BRANCH["Set Branch Variables"]
    BUILD_DOCS["build_docs_docker"]
    UPLOAD_DOCS["Upload via rsync"]
    UPLOAD_ALIAS["Upload Alias"]
    
    TRIGGER --> CHECKOUT
    CHECKOUT --> SET_BRANCH
    SET_BRANCH --> BUILD_DOCS
    BUILD_DOCS --> UPLOAD_DOCS
    UPLOAD_DOCS --> UPLOAD_ALIAS
    
    subgraph "Build Process"
        DOCKER["chesnay/flink-ci Container"]
        DOCS_SCRIPT["docs.sh Script"]
        HUGO_BUILD["Hugo Site Generation"]
        JAVADOC["Javadoc Generation"]
        PYTHON_DOCS["PyFlink Documentation"]
    end
    
    BUILD_DOCS --> DOCKER
    DOCKER --> DOCS_SCRIPT
    DOCS_SCRIPT --> HUGO_BUILD
    DOCS_SCRIPT --> JAVADOC
    DOCS_SCRIPT --> PYTHON_DOCS
```

The workflow supports multiple branches and creates aliases for different release versions:
- `master` branch → `release-2.1` alias
- `release-2.0` branch → `stable` alias  
- `release-1.20` branch → `lts` alias

Sources: [.github/workflows/docs.yml:16-74](), [.github/workflows/docs.sh:1-80]()

### Documentation Build Script

The `docs.sh` script orchestrates the complete documentation build process, including Hugo setup, external connector documentation integration, and API documentation generation.

Key operations performed by [.github/workflows/docs.sh:1-80]():
- Sets up Java 17 environment for builds
- Initializes Git submodules for external documentation
- Configures Hugo static site generator
- Builds Flink codebase for Javadoc generation
- Generates Java/Scala API documentation
- Builds PyFlink Python documentation when available

Sources: [.github/workflows/docs.sh:21-79](), [docs/setup_hugo.sh:20-43]()

## Azure Pipelines

### Main Pipeline Configuration

Azure Pipelines serves as the primary CI/CD system for comprehensive testing, nightly builds, and release processes. The pipeline system uses a templated approach to support multiple build configurations and testing scenarios.

```mermaid
graph TB
    subgraph "Pipeline Triggers"
        CRON["Nightly Schedule"]
        PR["Pull Request"]
        MANUAL["Manual Trigger"]
    end
    
    subgraph "Build Stages"
        CI_STAGE["CI Build Stage"]
        CRON_STAGE["Nightly Build Stage"]
        COMPILE["compile_stage_name"]
        TEST["test_stage_name"]
        E2E_1["e2e_1_stage_name"]
        E2E_2["e2e_2_stage_name"]
    end
    
    subgraph "Build Profiles"
        JDK11["JDK 11 Profile"]
        JDK17["JDK 17 Profile"]
        JDK21["JDK 21 Profile"]
        HADOOP313["Hadoop 3.1.3 Profile"]
        ADAPTIVE["Adaptive Scheduler Profile"]
    end
    
    CRON --> CRON_STAGE
    PR --> CI_STAGE
    MANUAL --> CI_STAGE
    
    CI_STAGE --> COMPILE
    CRON_STAGE --> COMPILE
    COMPILE --> TEST
    TEST --> E2E_1
    TEST --> E2E_2
    
    CRON_STAGE --> JDK11
    CRON_STAGE --> JDK17
    CRON_STAGE --> JDK21
    CRON_STAGE --> HADOOP313
    CRON_STAGE --> ADAPTIVE
```

Sources: [azure-pipelines.yml:32-101](), [tools/azure-pipelines/build-apache-repo.yml:24-179]()

### Job Template System

The `jobs-template.yml` defines reusable job configurations that support different hardware pools, environments, and test execution strategies.

Key template parameters include:
- `test_pool_definition`: Hardware configuration for compilation and unit tests
- `e2e_pool_definition`: Hardware configuration for end-to-end tests  
- `environment`: Environment variables and build profiles
- `jdk`: Java version specification
- `container`: Docker container for build environment

The template creates three main job types:
1. `compile_${stage_name}`: Compilation and build artifact creation
2. `test_${stage_name}`: Unit test execution across multiple modules
3. `e2e_${group}_${stage_name}`: End-to-end test execution

Sources: [tools/azure-pipelines/jobs-template.yml:16-198]()

### Test Module Organization

The pipeline organizes tests into logical modules to enable parallel execution and efficient resource utilization:

| Module | Components |
|--------|------------|
| `core` | Core runtime, streaming, state backends, RPC |
| `python` | PyFlink, Python API bridge |
| `table` | Table API, SQL planner, SQL client |
| `connect` | Connectors, file systems, formats |
| `tests` | Integration tests |
| `misc` | All remaining modules |

Sources: [tools/azure-pipelines/jobs-template.yml:90-103]()

## Build Infrastructure

### Compilation Process

The build infrastructure centers around the `compile.sh` script, which performs comprehensive quality assurance checks beyond basic compilation.

```mermaid
flowchart TD
    START["compile.sh Entry Point"]
    MVN_VERSION["Maven Version Check"]
    CLEAN_DEPLOY["mvn clean deploy"]
    JAVADOC["Javadoc Generation"]
    BUNDLED_CHECK["Bundled Dependencies Check"]
    SCALA_CHECK["Scala Suffixes Check"]
    SHADE_CHECK["Shaded Dependencies Check"]
    LICENSE_CHECK["License Validation"]
    
    START --> MVN_VERSION
    MVN_VERSION --> CLEAN_DEPLOY
    CLEAN_DEPLOY --> JAVADOC
    JAVADOC --> BUNDLED_CHECK
    BUNDLED_CHECK --> SCALA_CHECK
    SCALA_CHECK --> SHADE_CHECK
    SHADE_CHECK --> LICENSE_CHECK
    
    subgraph "Quality Checks"
        OPTIONAL["verify_bundled_optional.sh"]
        SUFFIXES["verify_scala_suffixes.sh"] 
        SHADED["shade.sh checks"]
        LICENSES["license_check.sh"]
    end
    
    BUNDLED_CHECK --> OPTIONAL
    SCALA_CHECK --> SUFFIXES
    SHADE_CHECK --> SHADED
    LICENSE_CHECK --> LICENSES
```

Sources: [tools/ci/compile.sh:55-120](), [tools/ci/compile_ci.sh:24-29]()

### Maven Utilities and Configuration

The `maven-utils.sh` script provides centralized Maven configuration and utility functions used throughout the build process.

Key functions include:
- `run_mvn`: Standardized Maven execution with logging
- `set_mirror_config`: Automatic mirror selection for faster builds
- `collect_coredumps`: Debug artifact collection for failed builds

The script automatically selects appropriate Maven mirrors based on availability:
- Alibaba mirror for faster builds in some regions
- Google mirror as fallback
- Configurable proxy settings for NPM dependencies

Sources: [tools/ci/maven-utils.sh:17-87]()

### Build Stage Organization

The `stage.sh` script defines module groupings for staged compilation and testing, enabling parallel builds and logical separation of concerns.

Module categories defined in [tools/ci/stage.sh:29-187]():
- `MODULES_CORE`: Core runtime components, state backends, RPC systems
- `MODULES_TABLE`: Table API, SQL processing, code generation
- `MODULES_CONNECTORS`: File systems, formats, connector implementations
- `MODULES_TESTS`: Integration and system tests

Functions `get_compile_modules_for_stage()` and `get_test_modules_for_stage()` provide Maven module selection logic for different build stages.

Sources: [tools/ci/stage.sh:20-187]()

## Documentation Generation Pipeline

### Hugo-Based Documentation System

Flink's documentation uses Hugo, a static site generator, configured through `config.toml` to support multiple languages and versioned documentation.

```mermaid
graph LR
    subgraph "Content Sources"
        CONTENT["docs/content/"]
        CONTENT_ZH["docs/content.zh/"]
        CONNECTORS["External Connector Docs"]
        JAVADOC["Generated Javadoc"]
        PYDOC["Generated PyDocs"]
    end
    
    subgraph "Hugo Processing"
        CONFIG["config.toml"]
        THEMES["Hugo Themes"]
        SHORTCODES["Custom Shortcodes"]
        HUGO_ENGINE["Hugo Static Generator"]
    end
    
    subgraph "Output"
        TARGET["docs/target/"]
        API_JAVA["api/java/"]
        API_PYTHON["api/python/"]
        STATIC_SITE["Static HTML Site"]
    end
    
    CONTENT --> HUGO_ENGINE
    CONTENT_ZH --> HUGO_ENGINE
    CONNECTORS --> HUGO_ENGINE
    CONFIG --> HUGO_ENGINE
    THEMES --> HUGO_ENGINE
    SHORTCODES --> HUGO_ENGINE
    
    HUGO_ENGINE --> TARGET
    JAVADOC --> API_JAVA
    PYDOC --> API_PYTHON
    TARGET --> STATIC_SITE
    API_JAVA --> STATIC_SITE
    API_PYTHON --> STATIC_SITE
```

Key configuration elements in [docs/config.toml:17-134]():
- Base URL configuration for different release branches
- Version parameters for documentation referencing
- Multi-language support (English and Chinese)
- External module integration for connector documentation

Sources: [docs/config.toml:17-134](), [docs/build_docs.sh:20-43]()

### External Documentation Integration

The documentation system supports integration of external connector documentation through the `setup_docs.sh` script. This allows connector repositories to maintain their own documentation while appearing seamlessly integrated in the main Flink documentation site.

Integration process includes:
1. Cloning external connector repositories
2. Processing connector-specific documentation
3. Mounting documentation content into Hugo themes
4. Generating unified navigation and cross-references

Sources: [docs/README.md:50-64](), [docs/build_docs.sh:28-46]()

### API Documentation Generation

API documentation generation involves multiple tools and processes:

**Java/Scala Documentation:**
- Maven `javadoc:aggregate` goal with custom configuration
- Custom header injection for site navigation
- Integration with main documentation site structure

**Python Documentation:**
- Sphinx-based documentation generation for PyFlink
- Conditional generation based on PyFlink availability
- Gateway-disabled mode for isolated documentation builds

Sources: [.github/workflows/docs.sh:54-79](), [tools/ci/compile.sh:82-95]()

## Testing and Quality Assurance

### End-to-End Test Orchestration

The E2E testing system uses containerized environments and supports multiple test groups for parallel execution.

```mermaid
graph TB
    subgraph "E2E Test Groups"
        GROUP1["E2E Group 1"]
        GROUP2["E2E Group 2"]
    end
    
    subgraph "Test Environment"
        CONTAINER["flink-build-container"]
        CACHE["Multi-level Caching"]
        DOCKER["Docker Image Management"]
    end
    
    subgraph "Test Execution"
        COMPILE_E2E["E2E Compilation"]
        RUN_TESTS["run-nightly-tests.sh"]
        UPLOAD_LOGS["Debug Artifact Upload"]
    end
    
    GROUP1 --> CONTAINER
    GROUP2 --> CONTAINER
    CONTAINER --> CACHE
    CACHE --> DOCKER
    DOCKER --> COMPILE_E2E
    COMPILE_E2E --> RUN_TESTS
    RUN_TESTS --> UPLOAD_LOGS
```

Key features of the E2E system include:
- Documentation-only change detection to skip unnecessary test runs
- Multi-level caching for Maven dependencies, E2E artifacts, and Docker images
- Kubernetes-based testing for cloud-native scenarios
- Automatic artifact upload for debugging failed tests

Sources: [tools/azure-pipelines/e2e-template.yml:24-141]()

### Quality Verification Scripts

Several specialized scripts ensure code quality and dependency management:

**Dependency Verification (`shade.sh`):**
- Validates shaded dependencies in distribution JAR
- Checks for prohibited unshaded libraries (ASM, Guava, Jackson)
- Verifies presence of required dependencies (Snappy)
- Validates S3 filesystem implementations

**License Compliance (`license_check.sh`):**
- Scans all deployed artifacts for license compliance
- Integrates with Maven build output parsing
- Ensures all distributed JARs meet Apache license requirements

**Scala Compatibility (`verify_scala_suffixes.sh`):**
- Analyzes Maven dependency trees for Scala dependencies
- Validates proper Scala version suffixes on artifacts
- Prevents Scala version conflicts in distribution

Sources: [tools/ci/shade.sh:23-151](), [tools/ci/license_check.sh:20-61](), [tools/ci/verify_scala_suffixes.sh:42-89]()

## Deployment and Release Processes

### Nightly Build System

The nightly build system creates snapshot releases and deploys them to distribution channels.

```mermaid
flowchart LR
    TRIGGER["Nightly Schedule"]
    BINARY_JOB["Binary Release Job"]
    MAVEN_JOB["Maven Snapshot Job"]
    
    subgraph "Binary Release"
        BUILD_BINARY["create_binary_release.sh"]
        S3_UPLOAD["S3 Artifact Upload"]
    end
    
    subgraph "Maven Deployment"
        DEPLOY_SETTINGS["Deploy Settings XML"]
        SNAPSHOT_DEPLOY["deploy_staging_jars.sh"]
        MAVEN_REPO["Apache Snapshot Repository"]
    end
    
    TRIGGER --> BINARY_JOB
    TRIGGER --> MAVEN_JOB
    
    BINARY_JOB --> BUILD_BINARY
    BUILD_BINARY --> S3_UPLOAD
    
    MAVEN_JOB --> DEPLOY_SETTINGS
    DEPLOY_SETTINGS --> SNAPSHOT_DEPLOY
    SNAPSHOT_DEPLOY --> MAVEN_REPO
```

The nightly system performs two parallel operations:
1. **Binary Release Generation**: Creates distribution packages and uploads to S3
2. **Maven Snapshot Deployment**: Deploys SNAPSHOT artifacts to Apache Maven repository

Sources: [tools/azure-pipelines/build-nightly-dist.yml:19-127]()

### Container and Environment Management

The CI/CD system relies on standardized container environments to ensure reproducible builds:

**Primary Build Container:**
- `chesnay/flink-ci:java_8_11_17_21_maven_386_jammy`
- Pre-configured with multiple JDK versions (8, 11, 17, 21)
- Maven 3.8.6 installation
- Ubuntu Jammy base with necessary build tools

**Environment Configuration:**
- Automatic JDK selection based on build profile
- Maven cache management with yearly cache invalidation
- Docker image caching for E2E tests
- APT mirror configuration for faster package installation

Sources: [tools/azure-pipelines/build-apache-repo.yml:38-43](), [tools/azure-pipelines/jobs-template.yml:56-64]()
