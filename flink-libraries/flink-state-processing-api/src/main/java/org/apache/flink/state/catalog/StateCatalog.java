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
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        if (discovery.find(databaseName).isEmpty()) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return Collections.emptyList();
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
        throw new TableNotExistException(getName(), tablePath);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        Optional<String> snapshotPath = discovery.find(tablePath.getDatabaseName());
        if (snapshotPath.isEmpty()) {
            return false;
        }
        return METADATA_TABLE.equals(tablePath.getObjectName());
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
}
