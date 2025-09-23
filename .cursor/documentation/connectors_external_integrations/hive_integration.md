# Hive Integration

<details>
<summary>Relevant source files</summary>

The following files were used as context for generating this wiki page:

- [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/CatalogTestBase.java](flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/CatalogTestBase.java)
- [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/GenericInMemoryCatalogTest.java](flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/GenericInMemoryCatalogTest.java)
- [flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/functions/sql/FlinkCurrentDateDynamicFunction.java](flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/functions/sql/FlinkCurrentDateDynamicFunction.java)
- [flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/functions/sql/FlinkCurrentRowTimestampFunction.java](flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/functions/sql/FlinkCurrentRowTimestampFunction.java)
- [flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/functions/sql/FlinkTimestampDynamicFunction.java](flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/functions/sql/FlinkTimestampDynamicFunction.java)
- [flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/functions/sql/FlinkTimestampWithPrecisionDynamicFunction.java](flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/functions/sql/FlinkTimestampWithPrecisionDynamicFunction.java)

</details>



## Purpose and Scope

This document covers Flink's Hive integration capabilities, focusing on catalog integration, query processing support, and the architectural foundations that enable Hive connectivity. The Hive integration allows Flink to interact with Hive metastores, execute queries using Hive dialect, and leverage existing Hive table definitions and metadata.

For general connector architecture and implementation patterns, see [Connector System](#5.1). For broader Table API and SQL processing capabilities, see [Table API & SQL](#3.1).

## Catalog Integration Architecture

Flink's Hive integration is built on top of the catalog abstraction, which provides a unified interface for accessing metadata from different sources. The catalog system supports databases, tables, partitions, views, functions, and statistics management.

### Catalog Abstraction Layer

The catalog system provides a common interface that supports both generic in-memory catalogs and external catalog implementations like Hive:

```mermaid
graph TB
    subgraph "Catalog Interface Layer"
        CI["Catalog Interface"]
        CB["CatalogTestBase"]
        CT["CatalogTest"]
    end
    
    subgraph "Generic Implementation"
        GIC["GenericInMemoryCatalog"]
        GICT["GenericInMemoryCatalogTest"]
    end
    
    subgraph "Hive Implementation"
        HC["HiveCatalog"]
        HCT["HiveCatalogTest"]
    end
    
    subgraph "Catalog Operations"
        DB["Database Management"]
        TBL["Table Management"]
        PART["Partition Management"]
        VIEW["View Management"]
        FUNC["Function Management"]
        STATS["Statistics Management"]
    end
    
    CI --> GIC
    CI --> HC
    CB --> GICT
    CB --> HCT
    
    GIC --> DB
    GIC --> TBL
    GIC --> PART
    GIC --> VIEW
    GIC --> FUNC
    GIC --> STATS
    
    HC --> DB
    HC --> TBL
    HC --> PART
    HC --> VIEW
    HC --> FUNC
    HC --> STATS
```

Sources: [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/CatalogTestBase.java:29-30](), [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/GenericInMemoryCatalogTest.java:47-53]()

### Connector Type Resolution

The catalog system distinguishes between generic and Hive-specific implementations through connector type resolution:

```mermaid
graph LR
    subgraph "Catalog Type Resolution"
        CG["isGeneric()"]
        CT["Connector Type"]
        COLL["COLLECTION"]
        HIVE["hive"]
    end
    
    subgraph "Table Properties"
        TP["Table Properties"]
        GF["Generic Flag"]
        CF["Connector Flag"]
    end
    
    CG -->|true| COLL
    CG -->|false| HIVE
    COLL --> CF
    HIVE --> CF
    CF --> TP
    GF --> TP
```

Sources: [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/CatalogTestBase.java:187-194](), [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/GenericInMemoryCatalogTest.java:215-217]()

## Hive vs Generic Catalog Distinction

The catalog implementation provides a clear distinction between generic and Hive-specific functionality through the `isGeneric()` method pattern:

| Aspect | Generic Catalog | Hive Catalog |
|--------|----------------|--------------|
| Connector Type | `COLLECTION` | `hive` |
| Implementation | `GenericInMemoryCatalog` | `HiveCatalog` |
| Test Base | `isGeneric() → true` | `isGeneric() → false` |
| Storage | In-memory | Hive Metastore |
| Metadata Persistence | Transient | Persistent |

The table properties generation demonstrates this distinction:

```mermaid
graph TB
    subgraph "Property Generation Flow"
        IGQ["isGeneric() Query"]
        BTP["getBatchTableProperties()"]
        STP["getStreamingTableProperties()"]
        GGF["getGenericFlag()"]
    end
    
    subgraph "Connector Selection"
        COND{"isGeneric?"}
        COLL["connector: COLLECTION"]
        HIVE["connector: hive"]
    end
    
    subgraph "Final Properties"
        IS["IS_STREAMING flag"]
        CF["Connector Flag"]
        FP["Final Properties Map"]
    end
    
    IGQ --> BTP
    IGQ --> STP
    BTP --> GGF
    STP --> GGF
    GGF --> COND
    COND -->|true| COLL
    COND -->|false| HIVE
    COLL --> CF
    HIVE --> CF
    IS --> FP
    CF --> FP
```

Sources: [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/CatalogTestBase.java:169-194]()

## Catalog Operations Support

The Hive integration supports comprehensive catalog operations through the same interface used by generic catalogs:

### Database and Table Management

```mermaid
graph TB
    subgraph "Database Operations"
        CDB["createDatabase()"]
        ADB["alterDatabase()"]
        DDB["dropDatabase()"]
        LDB["listDatabases()"]
    end
    
    subgraph "Table Operations"
        CT["createTable()"]
        AT["alterTable()"]
        DT["dropTable()"]
        RT["renameTable()"]
        LT["listTables()"]
        GT["getTable()"]
    end
    
    subgraph "Partition Operations"
        CP["createPartition()"]
        AP["alterPartition()"]
        DP["dropPartition()"]
        LP["listPartitions()"]
        GP["getPartition()"]
    end
    
    subgraph "Statistics Operations"
        ATS["alterTableStatistics()"]
        GTS["getTableStatistics()"]
        ATCS["alterTableColumnStatistics()"]
        GTCS["getTableColumnStatistics()"]
        BGS["bulkGetPartitionStatistics()"]
    end
    
    CDB --> CT
    CT --> CP
    CT --> ATS
    CP --> ATCS
    AT --> RT
    DT --> DP
```

Sources: [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/GenericInMemoryCatalogTest.java:58-140](), [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/GenericInMemoryCatalogTest.java:143-210]()

### View and Function Management

The catalog system also supports views and functions, which are essential for Hive compatibility:

```mermaid
graph LR
    subgraph "View Management"
        CV["createView()"]
        AV["alterView()"]
        DV["dropView()"]
        GV["getView()"]
    end
    
    subgraph "Function Management"
        CF["createFunction()"]
        AF["alterFunction()"]
        DF["dropFunction()"]
        GF["getFunction()"]
        LF["listFunctions()"]
    end
    
    subgraph "Model Management"
        CM["createModel()"]
        AM["alterModel()"]
        DM["dropModel()"]
        GM["getModel()"]
    end
    
    CV --> CF
    CF --> CM
    GV --> GF
    GF --> GM
```

Sources: [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/CatalogTestBase.java:142-167](), [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/GenericInMemoryCatalogTest.java:243-261]()

## Testing Infrastructure

The Hive integration leverages a comprehensive testing framework that ensures compatibility across different catalog implementations:

### Test Base Architecture

```mermaid
graph TB
    subgraph "Test Hierarchy"
        CT["CatalogTest"]
        CTB["CatalogTestBase"]
        GICT["GenericInMemoryCatalogTest"]
        HCT["HiveCatalogTest (implied)"]
    end
    
    subgraph "Test Factory Methods"
        CDB["createDb()"]
        CADB["createAnotherDb()"]
        CTM["createTable()"]
        CATM["createAnotherTable()"]
        CPT["createPartitionedTable()"]
        CP["createPartition()"]
        CV["createView()"]
        CF["createFunction()"]
    end
    
    subgraph "Test Verification"
        CE["checkEquals()"]
        TE["tableExists()"]
        PE["partitionExists()"]
        DE["databaseExists()"]
    end
    
    CT --> CTB
    CTB --> GICT
    CTB --> HCT
    
    CTB --> CDB
    CTB --> CADB
    CTB --> CTM
    CTB --> CATM
    CTB --> CPT
    CTB --> CP
    CTB --> CV
    CTB --> CF
    
    GICT --> CE
    GICT --> TE
    GICT --> PE
    GICT --> DE
```

Sources: [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/CatalogTestBase.java:30](), [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/GenericInMemoryCatalogTest.java:47]()

### Statistics Testing

The testing framework includes comprehensive statistics validation, which is crucial for Hive integration where table and partition statistics drive query optimization:

| Statistics Type | Table Level | Partition Level | Bulk Operations |
|----------------|-------------|-----------------|-----------------|
| Table Statistics | `alterTableStatistics()` | `alterPartitionStatistics()` | `bulkGetPartitionStatistics()` |
| Column Statistics | `alterTableColumnStatistics()` | `alterPartitionColumnStatistics()` | `bulkGetPartitionColumnStatistics()` |
| Validation | `getTableStatistics()` | `getPartitionStatistics()` | List-based retrieval |

Sources: [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/GenericInMemoryCatalogTest.java:96-140](), [flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/GenericInMemoryCatalogTest.java:143-210]()

## SQL Function Integration

The Hive integration includes support for SQL function compatibility, particularly around timestamp and date functions that may have different semantics between Hive and Flink:

### Dynamic Function Support

```mermaid
graph TB
    subgraph "Function Base Classes"
        SATF["SqlAbstractTimeFunction"]
        FTDF["FlinkTimestampDynamicFunction"]
        FCDF["FlinkCurrentDateDynamicFunction"]
        FCRTS["FlinkCurrentRowTimestampFunction"]
    end
    
    subgraph "Function Characteristics"
        BM["isBatchMode"]
        DYN["isDynamicFunction()"]
        DET["isDeterministic()"]
        RET["inferReturnType()"]
    end
    
    subgraph "Function Types"
        NOW["NOW()"]
        CT["CURRENT_TIMESTAMP"]
        CD["CURRENT_DATE"]
        LT["LOCALTIME"]
        CRT["CURRENT_ROW_TIMESTAMP"]
    end
    
    SATF --> FTDF
    FTDF --> FCDF
    SATF --> FCRTS
    
    FTDF --> BM
    BM --> DYN
    BM --> DET
    FTDF --> RET
    
    NOW --> FTDF
    CT --> FTDF
    CD --> FCDF
    LT --> FTDF
    CRT --> FCRTS
```

Sources: [flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/functions/sql/FlinkTimestampDynamicFunction.java:38-51](), [flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/functions/sql/FlinkCurrentDateDynamicFunction.java:34-51](), [flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/functions/sql/FlinkCurrentRowTimestampFunction.java:36-62]()

### Batch vs Streaming Function Behavior

The function implementations demonstrate how Hive compatibility is handled differently in batch and streaming contexts:

```mermaid
graph LR
    subgraph "Execution Mode Detection"
        EM["Execution Mode"]
        BM["Batch Mode"]
        SM["Streaming Mode"]
    end
    
    subgraph "Function Behavior"
        DYN["isDynamicFunction()"]
        DET["isDeterministic()"]
        PARENT["Parent Behavior"]
        CUSTOM["Custom Behavior"]
    end
    
    EM --> BM
    EM --> SM
    
    BM --> DYN
    BM --> DET
    DYN -->|true| PARENT
    DET -->|true| PARENT
    
    SM --> DYN
    SM --> DET
    DYN -->|false| CUSTOM
    DET -->|false| CUSTOM
```

Sources: [flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/functions/sql/FlinkTimestampDynamicFunction.java:49-57](), [flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/functions/sql/FlinkCurrentDateDynamicFunction.java:43-51]()

The Hive integration provides a robust foundation for accessing Hive metastores and executing Hive-compatible queries within Flink's unified batch and streaming processing framework.
