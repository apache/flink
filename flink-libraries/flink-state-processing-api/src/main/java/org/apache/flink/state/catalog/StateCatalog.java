/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.StateTableUtils;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.api.schema.KeyedStateSchemaInfo;
import org.apache.flink.state.table.SavepointConnectorOptions.StateReaderMode;
import org.apache.flink.state.table.SavepointConnectorOptions.StateType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A read-only Flink SQL catalog that discovers checkpoints and savepoints from a configured set of
 * directories and exposes their metadata as queryable SQL databases and views.
 *
 * <p>The catalog maps Flink's three-level hierarchy as follows:
 *
 * <ul>
 *   <li>Catalog: the name given at registration time (e.g. {@code "state"})
 *   <li>Database: one entry per discovered snapshot (e.g. {@code "app1_savepoint-acce1cedsad"})
 *   <li>Table: a single view named {@code "metadata"} per database, backed by the {@code
 *       savepoint_metadata} function from {@code StateModule}
 * </ul>
 *
 * <p>Database names preserve hyphens from the original directory names. Backtick quoting is
 * required in SQL for identifiers containing hyphens:
 *
 * <pre>{@code
 * USE CATALOG state;
 * USE `app1_savepoint-acce1cedsad`;
 * SELECT * FROM metadata;
 * }</pre>
 *
 * <p>{@code StateModule} must be loaded before querying any {@code metadata} view:
 *
 * <pre>{@code
 * tableEnv.loadModule("state", StateModule.INSTANCE);
 * }</pre>
 *
 * <p>Each catalog operation fetches state on demand. {@link #listDatabases()} performs a full
 * directory scan; all other operations perform a single file check on the specific snapshot path
 * reconstructed from the database name. There is no background polling and no shared cache.
 *
 * <p>All write operations throw {@link UnsupportedOperationException}.
 */
@PublicEvolving
public class StateCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(StateCatalog.class);

    public static final String METADATA_TABLE = "metadata";
    public static final String OPERATOR_UID_PREFIX = "uid_";
    public static final String OPERATOR_ID_PREFIX = "id_";
    public static final String OPERATOR_TABLE_SUFFIX = "_keyed";
    public static final String FLAT_STATE_TABLE_SUFFIX = "_keyed_flat";

    private static final CatalogDatabase EMPTY_DATABASE =
            new CatalogDatabaseImpl(Collections.emptyMap(), "");

    private final SnapshotDiscovery discovery;

    public StateCatalog(String name, Map<String, String> labelsToDirs) {
        this(name, labelsToDirs, StateCatalogOptions.LISTING_PARALLELISM.defaultValue());
    }

    public StateCatalog(String name, Map<String, String> labelsToDirs, int listingParallelism) {
        this(
                name,
                labelsToDirs,
                listingParallelism,
                StateCatalogOptions.DB_NAME_INCLUDE_TS.defaultValue());
    }

    public StateCatalog(
            String name,
            Map<String, String> labelsToDirs,
            int listingParallelism,
            boolean dbNameIncludeTs) {
        super(name, "default");
        this.discovery = new SnapshotDiscovery(labelsToDirs, listingParallelism, dbNameIncludeTs);
    }

    @Override
    @Nullable
    public String getDefaultDatabase() {
        return null;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void open() throws CatalogException {
        discovery.start();
        listDatabases();
    }

    @Override
    public void close() throws CatalogException {
        discovery.stop();
    }

    // -------------------------------------------------------------------------
    // Databases
    // -------------------------------------------------------------------------

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return discovery.list();
        } catch (IOException e) {
            LOG.warn("Failed to list databases in catalog '{}'", getName(), e);
            return Collections.emptyList();
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (discovery.find(databaseName).isEmpty()) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return EMPTY_DATABASE;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return discovery.find(databaseName).isPresent();
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    // -------------------------------------------------------------------------
    // Tables and views
    // -------------------------------------------------------------------------

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Optional<String> snapshotPath = discovery.find(databaseName);
        if (snapshotPath.isEmpty()) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        List<String> tables = new ArrayList<>();
        tables.add(METADATA_TABLE);
        Set<String> seen = new LinkedHashSet<>();
        try {
            CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(snapshotPath.get());
            for (OperatorIdentifier opId : StateTableUtils.getOperatorIdentifiers(metadata)) {
                for (ResolvedTable candidate : candidateTablesForOperator(metadata, opId)) {
                    String name =
                            tableName(
                                    candidate.operatorIdentifier,
                                    candidate.kind,
                                    candidate.stateName);
                    // Two distinct (operator, state) combinations can legitimately derive the
                    // same table name, since operator UIDs and state names may themselves
                    // contain underscores (see #tableName). Skip and warn rather than exposing
                    // the same name twice, mirroring how SnapshotDiscovery#list handles
                    // colliding database names.
                    if (!seen.add(name)) {
                        LOG.warn(
                                "Table name '{}' is ambiguous between multiple operators/states "
                                        + "in database '{}' and only the first one found is "
                                        + "exposed. Consider renaming the colliding operator "
                                        + "UID(s) or state name(s).",
                                name,
                                databaseName);
                        continue;
                    }
                    tables.add(name);
                }
            }
        } catch (IOException e) {
            throw new CatalogException(
                    "Failed to load checkpoint metadata for database '" + databaseName + "'", e);
        }
        return tables;
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (discovery.find(databaseName).isEmpty()) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return Collections.singletonList(METADATA_TABLE);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        Optional<String> snapshotPath = discovery.find(tablePath.getDatabaseName());
        if (snapshotPath.isEmpty()) {
            throw new TableNotExistException(getName(), tablePath);
        }
        String tableName = tablePath.getObjectName();
        if (METADATA_TABLE.equals(tableName)) {
            return buildMetadataView(snapshotPath.get());
        }
        try {
            CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(snapshotPath.get());
            ResolvedTable resolved =
                    resolveTable(metadata, tableName)
                            .orElseThrow(() -> new TableNotExistException(getName(), tablePath));
            switch (resolved.kind) {
                case KEYED:
                    {
                        KeyedStateSchemaInfo schemaInfo =
                                StateTableUtils.getKeyedStateSchema(
                                        metadata, resolved.operatorIdentifier);
                        return StateTableUtils.getStateCatalogTable(
                                metadata,
                                schemaInfo,
                                snapshotPath.get(),
                                resolved.operatorIdentifier);
                    }
                case KEYED_FLAT:
                    {
                        KeyedStateSchemaInfo schemaInfo =
                                StateTableUtils.getKeyedStateSchema(
                                        metadata, resolved.operatorIdentifier);
                        return StateTableUtils.getFlattenedStateCatalogTable(
                                metadata,
                                schemaInfo,
                                resolved.stateName,
                                snapshotPath.get(),
                                resolved.operatorIdentifier);
                    }
                default:
                    throw new IllegalStateException("Unhandled table kind " + resolved.kind);
            }
        } catch (IOException e) {
            throw new CatalogException(
                    "Failed to load state schema for table '" + tablePath + "'", e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        Optional<String> snapshotPath = discovery.find(tablePath.getDatabaseName());
        if (snapshotPath.isEmpty()) {
            return false;
        }
        String tableName = tablePath.getObjectName();
        if (METADATA_TABLE.equals(tableName)) {
            return true;
        }
        if (!tableName.startsWith(OPERATOR_UID_PREFIX)
                && !tableName.startsWith(OPERATOR_ID_PREFIX)) {
            return false;
        }
        try {
            CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(snapshotPath.get());
            return resolveTable(metadata, tableName).isPresent();
        } catch (IOException e) {
            LOG.warn(
                    "Failed to load checkpoint metadata while checking existence of table '{}'",
                    tablePath,
                    e);
            return false;
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableAlreadyExistException, TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    // -------------------------------------------------------------------------
    // Partitions (not supported)
    // -------------------------------------------------------------------------

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        throw new TableNotPartitionedException(getName(), tablePath);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return listPartitions(tablePath);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return listPartitions(tablePath);
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException,
                    TableNotPartitionedException,
                    PartitionSpecInvalidException,
                    PartitionAlreadyExistsException,
                    CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    // -------------------------------------------------------------------------
    // Functions (not supported)
    // -------------------------------------------------------------------------

    @Override
    public List<String> listFunctions(String dbName)
            throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    // -------------------------------------------------------------------------
    // Statistics (read-only stubs)
    // -------------------------------------------------------------------------

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("StateCatalog is read-only.");
    }

    // -------------------------------------------------------------------------
    // View construction
    // -------------------------------------------------------------------------

    private static CatalogView buildMetadataView(String snapshotPath) {
        String escapedPath = snapshotPath.replace("'", "''");
        String query = String.format("SELECT * FROM TABLE(savepoint_metadata('%s'))", escapedPath);
        // Once the upstream StateCatalog PR is merged and OUTPUT_DATA_TYPE is available in
        // SavepointMetadataTableFunction, replace the schema definition below with:
        //   Schema.newBuilder()
        //       .fromRowDataType(SavepointMetadataTableFunction.OUTPUT_DATA_TYPE)
        //       .build()

        Schema schema =
                Schema.newBuilder()
                        .fromRowDataType(
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "checkpoint-id", DataTypes.BIGINT().notNull()),
                                        DataTypes.FIELD("operator-name", DataTypes.STRING()),
                                        DataTypes.FIELD("operator-uid", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "operator-uid-hash", DataTypes.STRING().notNull()),
                                        DataTypes.FIELD(
                                                "operator-parallelism", DataTypes.INT().notNull()),
                                        DataTypes.FIELD(
                                                "operator-max-parallelism",
                                                DataTypes.INT().notNull()),
                                        DataTypes.FIELD(
                                                "operator-subtask-state-count",
                                                DataTypes.INT().notNull()),
                                        DataTypes.FIELD(
                                                "operator-coordinator-state-size-in-bytes",
                                                DataTypes.BIGINT().notNull()),
                                        DataTypes.FIELD(
                                                "operator-total-size-in-bytes",
                                                DataTypes.BIGINT().notNull())))
                        .build();
        return CatalogView.of(
                schema,
                "Operator metadata for snapshot at " + snapshotPath,
                query,
                query,
                Collections.emptyMap());
    }

    // -------------------------------------------------------------------------
    // Operator table helpers
    // -------------------------------------------------------------------------

    /**
     * Table name for a {@code kind} of operator state, optionally scoped to one flattened/non-keyed
     * state (see {@link #OPERATOR_TABLE_SUFFIX}/{@link #FLAT_STATE_TABLE_SUFFIX}).
     *
     * <p>{@code stateName} must be {@code null} for {@link StateReaderMode#KEYED}/{@link
     * StateReaderMode#WINDOWED} (the general keyed/namespaced table, one per operator) and non-null
     * for every other kind (a table scoped to one flattened LIST/MAP state, or one non-keyed state
     * — the state name alone disambiguates the table since keyed/non-keyed state names are unique
     * within an operator).
     */
    private static final Map<StateReaderMode, String> TABLE_SUFFIXES =
            Map.of(
                    StateReaderMode.KEYED, OPERATOR_TABLE_SUFFIX,
                    StateReaderMode.KEYED_FLAT, FLAT_STATE_TABLE_SUFFIX);

    static String tableName(
            OperatorIdentifier opId, StateReaderMode kind, @Nullable String stateName) {
        String base =
                opId.getUid()
                        .map(uid -> OPERATOR_UID_PREFIX + uid)
                        .orElseGet(() -> OPERATOR_ID_PREFIX + opId.getOperatorId().toHexString());
        String suffix = TABLE_SUFFIXES.get(kind);
        if (suffix == null) {
            throw new IllegalArgumentException("Unknown state reader mode: " + kind);
        }
        return stateName == null ? base + suffix : base + "_" + stateName + suffix;
    }

    /**
     * Identifies which table a table name refers to: the operator, the table shape ({@link
     * StateReaderMode}), and — for flattened tables — the name of the flattened state.
     */
    private static final class ResolvedTable {
        final OperatorIdentifier operatorIdentifier;
        final StateReaderMode kind;
        @Nullable final String stateName;

        ResolvedTable(OperatorIdentifier operatorIdentifier, StateReaderMode kind) {
            this(operatorIdentifier, kind, null);
        }

        ResolvedTable(
                OperatorIdentifier operatorIdentifier,
                StateReaderMode kind,
                @Nullable String stateName) {
            this.operatorIdentifier = operatorIdentifier;
            this.kind = kind;
            this.stateName = stateName;
        }
    }

    /**
     * Resolves a table name to the operator (and, for flattened tables, the state) it refers to, by
     * generating candidate names for every operator/state in the checkpoint and matching against
     * {@code tableName}. Names cannot be parsed directly since operator UIDs and state names may
     * themselves contain underscores.
     */
    private static Optional<ResolvedTable> resolveTable(
            CheckpointMetadata metadata, String tableName) {
        for (OperatorIdentifier opId : StateTableUtils.getOperatorIdentifiers(metadata)) {
            List<ResolvedTable> candidates;
            try {
                candidates = candidateTablesForOperator(metadata, opId);
            } catch (IOException e) {
                LOG.warn("Failed to load state schema for operator '{}'. Skipping.", opId, e);
                continue;
            }
            for (ResolvedTable candidate : candidates) {
                if (tableName(candidate.operatorIdentifier, candidate.kind, candidate.stateName)
                        .equals(tableName)) {
                    return Optional.of(candidate);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Enumerates every table that {@code opId} contributes: the general keyed table (if any plain
     * per-key state is registered), plus one flattened table per LIST/MAP keyed state.
     *
     * <p>Shared by {@link #listTables} (which collects names for every candidate) and {@link
     * #resolveTable} (which matches candidates against a target name), so that adding a new state
     * kind only requires updating this one traversal.
     */
    private static List<ResolvedTable> candidateTablesForOperator(
            CheckpointMetadata metadata, OperatorIdentifier opId) throws IOException {
        List<ResolvedTable> candidates = new ArrayList<>();

        KeyedStateSchemaInfo schemaInfo = StateTableUtils.getKeyedStateSchema(metadata, opId);
        if (!schemaInfo.stateSchemas.isEmpty()) {
            candidates.add(new ResolvedTable(opId, StateReaderMode.KEYED));
            for (Map.Entry<String, KeyedStateSchemaInfo.StateEntryInfo> entry :
                    schemaInfo.stateSchemas.entrySet()) {
                StateType stateType = entry.getValue().stateType;
                if (stateType == StateType.LIST || stateType == StateType.MAP) {
                    candidates.add(
                            new ResolvedTable(opId, StateReaderMode.KEYED_FLAT, entry.getKey()));
                }
            }
        }

        return candidates;
    }
}
