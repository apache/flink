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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.connectors.hive.HiveDynamicTableFactory;
import org.apache.flink.connectors.hive.HiveTableFactory;
import org.apache.flink.connectors.hive.util.HivePartitionUtils;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ManagedTableListener;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
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
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.table.catalog.hive.factories.HiveFunctionDefinitionFactory;
import org.apache.flink.table.catalog.hive.util.AlterHiveDatabaseOp;
import org.apache.flink.table.catalog.hive.util.AlterTableOp;
import org.apache.flink.table.catalog.hive.util.HiveDDLUtils;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.catalog.hive.util.HiveStatsUtil;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.factories.ManagedTableFactory;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX;
import static org.apache.flink.table.catalog.hive.util.Constants.ALTER_COL_CASCADE;
import static org.apache.flink.table.catalog.hive.util.Constants.ALTER_DATABASE_OP;
import static org.apache.flink.table.catalog.hive.util.Constants.ALTER_TABLE_OP;
import static org.apache.flink.table.catalog.hive.util.Constants.DATABASE_LOCATION_URI;
import static org.apache.flink.table.catalog.hive.util.Constants.DATABASE_OWNER_NAME;
import static org.apache.flink.table.catalog.hive.util.Constants.DATABASE_OWNER_TYPE;
import static org.apache.flink.table.catalog.hive.util.Constants.IDENTIFIER;
import static org.apache.flink.table.catalog.hive.util.Constants.NOT_NULL_COLS;
import static org.apache.flink.table.catalog.hive.util.Constants.NOT_NULL_CONSTRAINT_TRAITS;
import static org.apache.flink.table.catalog.hive.util.Constants.PK_CONSTRAINT_TRAIT;
import static org.apache.flink.table.catalog.hive.util.Constants.ROLE_OWNER;
import static org.apache.flink.table.catalog.hive.util.Constants.STORED_AS_FILE_FORMAT;
import static org.apache.flink.table.catalog.hive.util.Constants.TABLE_LOCATION_URI;
import static org.apache.flink.table.catalog.hive.util.Constants.USER_OWNER;
import static org.apache.flink.table.catalog.hive.util.HiveTableUtil.getHadoopConfiguration;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** A catalog implementation for Hive. */
public class HiveCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

    // Default database of Hive metastore
    public static final String DEFAULT_DB = "default";

    public static final String HIVE_SITE_FILE = "hive-site.xml";

    // Prefix used to distinguish scala/python functions
    private static final String FLINK_SCALA_FUNCTION_PREFIX = "flink:scala:";
    private static final String FLINK_PYTHON_FUNCTION_PREFIX = "flink:python:";

    private final HiveConf hiveConf;
    private final String hiveVersion;
    private final HiveShim hiveShim;

    @VisibleForTesting HiveMetastoreClientWrapper client;

    public HiveCatalog(
            String catalogName, @Nullable String defaultDatabase, @Nullable String hiveConfDir) {
        this(catalogName, defaultDatabase, hiveConfDir, null);
    }

    public HiveCatalog(
            String catalogName,
            @Nullable String defaultDatabase,
            @Nullable String hiveConfDir,
            String hiveVersion) {
        this(catalogName, defaultDatabase, hiveConfDir, null, hiveVersion);
    }

    public HiveCatalog(
            String catalogName,
            @Nullable String defaultDatabase,
            @Nullable String hiveConfDir,
            @Nullable String hadoopConfDir,
            @Nullable String hiveVersion) {
        this(catalogName, defaultDatabase, createHiveConf(hiveConfDir, hadoopConfDir), hiveVersion);
    }

    public HiveCatalog(
            String catalogName,
            @Nullable String defaultDatabase,
            @Nullable HiveConf hiveConf,
            @Nullable String hiveVersion) {
        this(
                catalogName,
                defaultDatabase,
                hiveConf,
                isNullOrWhitespaceOnly(hiveVersion) ? HiveShimLoader.getHiveVersion() : hiveVersion,
                false);
    }

    public HiveCatalog(
            String catalogName,
            @Nullable String defaultDatabase,
            @Nullable HiveConf hiveConf,
            String hiveVersion,
            boolean allowEmbedded) {
        super(catalogName, defaultDatabase == null ? DEFAULT_DB : defaultDatabase);

        this.hiveConf = hiveConf == null ? createHiveConf(null, null) : hiveConf;
        if (!allowEmbedded) {
            checkArgument(
                    !isEmbeddedMetastore(this.hiveConf),
                    "Embedded metastore is not allowed. Make sure you have set a valid value for "
                            + HiveConf.ConfVars.METASTOREURIS.toString());
        }
        checkArgument(!isNullOrWhitespaceOnly(hiveVersion), "hiveVersion cannot be null or empty");
        this.hiveVersion = hiveVersion;
        hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
        // add this to hiveConf to make sure table factory and source/sink see the same Hive version
        // as HiveCatalog
        this.hiveConf.set(HiveCatalogFactoryOptions.HIVE_VERSION.key(), hiveVersion);

        LOG.info("Created HiveCatalog '{}'", catalogName);
    }

    public static HiveConf createHiveConf(
            @Nullable String hiveConfDir, @Nullable String hadoopConfDir) {
        // create HiveConf from hadoop configuration with hadoop conf directory configured.
        Configuration hadoopConf = null;
        if (isNullOrWhitespaceOnly(hadoopConfDir)) {
            for (String possibleHadoopConfPath :
                    HadoopUtils.possibleHadoopConfPaths(
                            new org.apache.flink.configuration.Configuration())) {
                hadoopConf = getHadoopConfiguration(possibleHadoopConfPath);
                if (hadoopConf != null) {
                    break;
                }
            }
        } else {
            hadoopConf = getHadoopConfiguration(hadoopConfDir);
            if (hadoopConf == null) {
                String possiableUsedConfFiles =
                        "core-site.xml | hdfs-site.xml | yarn-site.xml | mapred-site.xml";
                throw new CatalogException(
                        "Failed to load the hadoop conf from specified path:" + hadoopConfDir,
                        new FileNotFoundException(
                                "Please check the path none of the conf files ("
                                        + possiableUsedConfFiles
                                        + ") exist in the folder."));
            }
        }
        if (hadoopConf == null) {
            hadoopConf = new Configuration();
        }
        // ignore all the static conf file URLs that HiveConf may have set
        HiveConf.setHiveSiteLocation(null);
        HiveConf.setLoadMetastoreConfig(false);
        HiveConf.setLoadHiveServer2Config(false);
        HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);

        LOG.info("Setting hive conf dir as {}", hiveConfDir);

        if (hiveConfDir != null) {
            Path hiveSite = new Path(hiveConfDir, HIVE_SITE_FILE);
            if (!hiveSite.toUri().isAbsolute()) {
                // treat relative URI as local file to be compatible with previous behavior
                hiveSite = new Path(new File(hiveSite.toString()).toURI());
            }
            try (InputStream inputStream = hiveSite.getFileSystem(hadoopConf).open(hiveSite)) {
                hiveConf.addResource(inputStream, hiveSite.toString());
                // trigger a read from the conf so that the input stream is read
                isEmbeddedMetastore(hiveConf);
            } catch (IOException e) {
                throw new CatalogException(
                        "Failed to load hive-site.xml from specified path:" + hiveSite, e);
            }
        } else {
            // user doesn't provide hive conf dir, we try to find it in classpath
            URL hiveSite =
                    Thread.currentThread().getContextClassLoader().getResource(HIVE_SITE_FILE);
            if (hiveSite != null) {
                LOG.info("Found {} in classpath: {}", HIVE_SITE_FILE, hiveSite);
                hiveConf.addResource(hiveSite);
            }
        }
        return hiveConf;
    }

    public HiveConf getHiveConf() {
        return hiveConf;
    }

    @Internal
    public String getHiveVersion() {
        return hiveVersion;
    }

    @Override
    public void open() throws CatalogException {
        if (client == null) {
            client = HiveMetastoreClientFactory.create(hiveConf, hiveVersion);
            LOG.info("Connected to Hive metastore");
        }

        if (!databaseExists(getDefaultDatabase())) {
            throw new CatalogException(
                    String.format(
                            "Configured default database %s doesn't exist in catalog %s.",
                            getDefaultDatabase(), getName()));
        }
    }

    @Override
    public void close() throws CatalogException {
        if (client != null) {
            client.close();
            client = null;
            LOG.info("Close connection to Hive metastore");
        }
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new HiveDynamicTableFactory(hiveConf));
    }

    @Override
    public Optional<TableFactory> getTableFactory() {
        return Optional.of(new HiveTableFactory());
    }

    @Override
    public Optional<FunctionDefinitionFactory> getFunctionDefinitionFactory() {
        return Optional.of(new HiveFunctionDefinitionFactory(hiveShim));
    }

    // ------ databases ------

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Database hiveDatabase = getHiveDatabase(databaseName);

        Map<String, String> properties = new HashMap<>(hiveDatabase.getParameters());

        properties.put(DATABASE_LOCATION_URI, hiveDatabase.getLocationUri());

        return new CatalogDatabaseImpl(properties, hiveDatabase.getDescription());
    }

    @Override
    public void createDatabase(
            String databaseName, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        checkArgument(
                !isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        checkNotNull(database, "database cannot be null");

        Map<String, String> properties = database.getProperties();

        String dbLocationUri = properties.remove(DATABASE_LOCATION_URI);

        Database hiveDatabase =
                new Database(databaseName, database.getComment(), dbLocationUri, properties);

        try {
            client.createDatabase(hiveDatabase);
        } catch (AlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(getName(), hiveDatabase.getName());
            }
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to create database %s", hiveDatabase.getName()), e);
        }
    }

    @Override
    public void alterDatabase(
            String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        checkNotNull(newDatabase, "newDatabase cannot be null");

        // client.alterDatabase doesn't throw any exception if there is no existing database
        Database hiveDB;
        try {
            hiveDB = getHiveDatabase(databaseName);
        } catch (DatabaseNotExistException e) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(getName(), databaseName);
            }

            return;
        }

        try {
            client.alterDatabase(databaseName, alterDatabase(hiveDB, newDatabase));
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to alter database %s", databaseName), e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return client.getAllDatabases();
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to list all databases in %s", getName()), e);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            return client.getDatabase(databaseName) != null;
        } catch (NoSuchObjectException e) {
            return false;
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to determine whether database %s exists or not", databaseName),
                    e);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        try {
            client.dropDatabase(name, true, ignoreIfNotExists, cascade);
        } catch (NoSuchObjectException e) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(getName(), name);
            }
        } catch (InvalidOperationException e) {
            throw new DatabaseNotEmptyException(getName(), name);
        } catch (TException e) {
            throw new CatalogException(String.format("Failed to drop database %s", name), e);
        }
    }

    @VisibleForTesting
    public Database getHiveDatabase(String databaseName) throws DatabaseNotExistException {
        try {
            return client.getDatabase(databaseName);
        } catch (NoSuchObjectException e) {
            throw new DatabaseNotExistException(getName(), databaseName);
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to get database %s from %s", databaseName, getName()), e);
        }
    }

    // ------ tables ------

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");

        Table hiveTable = getHiveTable(tablePath);
        return instantiateCatalogTable(hiveTable);
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        checkNotNull(table, "table cannot be null");
        checkArgument(table instanceof ResolvedCatalogBaseTable, "table should be resolved");

        ResolvedCatalogBaseTable<?> resolvedTable = (ResolvedCatalogBaseTable<?>) table;
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }

        boolean managedTable = ManagedTableListener.isManagedTable(this, resolvedTable);
        Table hiveTable =
                HiveTableUtil.instantiateHiveTable(
                        tablePath, resolvedTable, hiveConf, managedTable);

        UniqueConstraint pkConstraint = null;
        ResolvedSchema resolvedSchema = resolvedTable.getResolvedSchema();
        List<String> notNullCols = new ArrayList<>();
        boolean isHiveTable = isHiveTable(resolvedTable.getOptions());
        if (isHiveTable) {
            org.apache.flink.table.catalog.UniqueConstraint uk =
                    resolvedSchema.getPrimaryKey().orElse(null);
            pkConstraint =
                    uk == null ? null : UniqueConstraint.primaryKey(uk.getName(), uk.getColumns());
            String nnColStr = hiveTable.getParameters().remove(NOT_NULL_COLS);
            if (nnColStr != null) {
                notNullCols.addAll(Arrays.asList(nnColStr.split(HiveDDLUtils.COL_DELIMITER)));
            } else {
                for (int i = 0; i < resolvedSchema.getColumnCount(); i++) {
                    if (!resolvedSchema.getColumnDataTypes().get(i).getLogicalType().isNullable()) {
                        notNullCols.add(resolvedSchema.getColumns().get(i).getName());
                    }
                }
            }
            // remove the 'connector' option for hive table
            hiveTable.getParameters().remove(CONNECTOR.key());
        }

        try {
            if (pkConstraint != null || !notNullCols.isEmpty()) {
                // extract constraint traits from table properties
                String pkTraitStr = hiveTable.getParameters().remove(PK_CONSTRAINT_TRAIT);
                byte pkTrait =
                        pkTraitStr == null
                                ? HiveDDLUtils.defaultTrait()
                                : Byte.parseByte(pkTraitStr);
                List<Byte> pkTraits =
                        Collections.nCopies(
                                pkConstraint == null ? 0 : pkConstraint.getColumns().size(),
                                pkTrait);

                List<Byte> nnTraits;
                String nnTraitsStr = hiveTable.getParameters().remove(NOT_NULL_CONSTRAINT_TRAITS);
                if (nnTraitsStr != null) {
                    String[] traits = nnTraitsStr.split(HiveDDLUtils.COL_DELIMITER);
                    Preconditions.checkArgument(
                            traits.length == notNullCols.size(),
                            "Number of NOT NULL columns and constraint traits mismatch");
                    nnTraits = Arrays.stream(traits).map(Byte::new).collect(Collectors.toList());
                } else {
                    nnTraits = Collections.nCopies(notNullCols.size(), HiveDDLUtils.defaultTrait());
                }

                client.createTableWithConstraints(
                        hiveTable, hiveConf, pkConstraint, pkTraits, notNullCols, nnTraits);
            } else {
                client.createTable(hiveTable);
            }
        } catch (AlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(getName(), tablePath, e);
            }
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to create table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        checkArgument(
                !isNullOrWhitespaceOnly(newTableName), "newTableName cannot be null or empty");

        try {
            // alter_table() doesn't throw a clear exception when target table doesn't exist.
            // Thus, check the table existence explicitly
            if (tableExists(tablePath)) {
                ObjectPath newPath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
                // alter_table() doesn't throw a clear exception when new table already exists.
                // Thus, check the table existence explicitly
                if (tableExists(newPath)) {
                    throw new TableAlreadyExistException(getName(), newPath);
                } else {
                    Table table = getHiveTable(tablePath);
                    table.setTableName(newTableName);
                    client.alter_table(
                            tablePath.getDatabaseName(), tablePath.getObjectName(), table);
                }
            } else if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to rename table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newCatalogTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        checkNotNull(newCatalogTable, "newCatalogTable cannot be null");
        checkArgument(
                newCatalogTable instanceof ResolvedCatalogBaseTable, "table should be resolved");

        Table hiveTable;
        try {
            hiveTable = getHiveTable(tablePath);
        } catch (TableNotExistException e) {
            if (!ignoreIfNotExists) {
                throw e;
            }
            return;
        }

        CatalogBaseTable existingTable = instantiateCatalogTable(hiveTable);

        if (existingTable.getTableKind() != newCatalogTable.getTableKind()) {
            throw new CatalogException(
                    String.format(
                            "Table types don't match. Existing table is '%s' and new table is '%s'.",
                            existingTable.getTableKind(), newCatalogTable.getTableKind()));
        }

        disallowChangeCatalogTableType(existingTable.getOptions(), newCatalogTable.getOptions());

        boolean isHiveTable = isHiveTable(hiveTable.getParameters());
        if (isHiveTable) {
            AlterTableOp op = HiveTableUtil.extractAlterTableOp(newCatalogTable.getOptions());
            if (op == null) {
                // the alter operation isn't encoded as properties
                hiveTable =
                        HiveTableUtil.alterTableViaCatalogBaseTable(
                                tablePath,
                                (ResolvedCatalogBaseTable) newCatalogTable,
                                hiveTable,
                                hiveConf,
                                false);
            } else {
                alterTableViaProperties(
                        op,
                        hiveTable,
                        (ResolvedCatalogTable) newCatalogTable,
                        hiveTable.getParameters(),
                        newCatalogTable.getOptions(),
                        hiveTable.getSd());
            }
        } else {
            hiveTable =
                    HiveTableUtil.alterTableViaCatalogBaseTable(
                            tablePath,
                            (ResolvedCatalogBaseTable) newCatalogTable,
                            hiveTable,
                            hiveConf,
                            ManagedTableListener.isManagedTable(this, newCatalogTable));
        }
        if (isHiveTable) {
            hiveTable.getParameters().remove(CONNECTOR.key());
        }
        try {
            client.alter_table(tablePath.getDatabaseName(), tablePath.getObjectName(), hiveTable);
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to alter table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");

        try {
            client.dropTable(
                    tablePath.getDatabaseName(),
                    tablePath.getObjectName(),
                    // Indicate whether associated data should be deleted.
                    // Set to 'true' for now because Flink tables shouldn't have data in Hive. Can
                    // be changed later if necessary
                    true,
                    ignoreIfNotExists);
        } catch (NoSuchObjectException e) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to drop table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

        try {
            return client.getAllTables(databaseName);
        } catch (UnknownDBException e) {
            throw new DatabaseNotExistException(getName(), databaseName);
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to list tables in database %s", databaseName), e);
        }
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

        try {
            return client.getViews(databaseName);
        } catch (UnknownDBException e) {
            throw new DatabaseNotExistException(getName(), databaseName);
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to list views in database %s", databaseName), e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");

        try {
            return client.tableExists(tablePath.getDatabaseName(), tablePath.getObjectName());
        } catch (UnknownDBException e) {
            return false;
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to check whether table %s exists or not.",
                            tablePath.getFullName()),
                    e);
        }
    }

    @Internal
    public Table getHiveTable(ObjectPath tablePath) throws TableNotExistException {
        try {
            Table table = client.getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
            boolean isHiveTable = isHiveTable(table);
            // for hive table, we add the connector property
            if (isHiveTable) {
                table.getParameters().put(CONNECTOR.key(), IDENTIFIER);
            }
            return table;
        } catch (NoSuchObjectException e) {
            throw new TableNotExistException(getName(), tablePath);
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to get table %s from Hive metastore", tablePath.getFullName()),
                    e);
        }
    }

    @VisibleForTesting
    CatalogBaseTable instantiateCatalogTable(Table hiveTable) {
        boolean isView = TableType.valueOf(hiveTable.getTableType()) == TableType.VIRTUAL_VIEW;

        // Table properties
        Map<String, String> properties = new HashMap<>(hiveTable.getParameters());

        boolean isHiveTable = isHiveTable(properties);

        // Partition keys
        List<String> partitionKeys = new ArrayList<>();
        org.apache.flink.table.api.Schema schema;
        String comment;
        if (isHiveTable) {
            // Table schema
            schema = HiveTableUtil.createSchema(hiveConf, hiveTable, client, hiveShim);

            if (!hiveTable.getPartitionKeys().isEmpty()) {
                partitionKeys = getFieldNames(hiveTable.getPartitionKeys());
            }
            comment = properties.remove(HiveCatalogConfig.COMMENT);
        } else {
            properties = retrieveFlinkProperties(properties);

            if (ManagedTableFactory.DEFAULT_IDENTIFIER.equalsIgnoreCase(
                    properties.get(CONNECTOR.key()))) {
                // for Flink's managed table, we remove the connector option
                properties.remove(CONNECTOR.key());
            }

            CatalogTable catalogTable = CatalogPropertiesUtil.deserializeCatalogTable(properties);
            if (catalogTable.getUnresolvedSchema().getColumns().isEmpty()) {
                // try to get table schema with both new and old (1.10) key, in order to support
                // tables created in old version
                catalogTable =
                        CatalogPropertiesUtil.deserializeCatalogTable(
                                properties, "generic.table.schema");
            }

            schema = catalogTable.getUnresolvedSchema();
            partitionKeys = catalogTable.getPartitionKeys();
            comment = properties.remove(HiveCatalogConfig.COMMENT);
            properties = catalogTable.getOptions();
        }

        if (isView) {
            return CatalogView.of(
                    schema,
                    comment,
                    hiveTable.getViewOriginalText(),
                    hiveTable.getViewExpandedText(),
                    properties);
        } else {
            return CatalogTable.of(schema, comment, partitionKeys, properties);
        }
    }

    /** Filter out Hive-created properties, and return Flink-created properties. */
    private static Map<String, String> retrieveFlinkProperties(
            Map<String, String> hiveTableParams) {
        return hiveTableParams.entrySet().stream()
                .filter(e -> e.getKey().startsWith(FLINK_PROPERTY_PREFIX))
                .collect(
                        Collectors.toMap(
                                e -> e.getKey().substring(FLINK_PROPERTY_PREFIX.length()),
                                e -> e.getValue()));
    }

    // ------ partitions ------

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

        try {
            return getHivePartition(tablePath, partitionSpec) != null;
        } catch (NoSuchObjectException | TableNotExistException | PartitionSpecInvalidException e) {
            return false;
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to get partition %s of table %s", partitionSpec, tablePath),
                    e);
        }
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, PartitionAlreadyExistsException,
                    CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");
        checkNotNull(partition, "Partition cannot be null");

        Table hiveTable = getHiveTable(tablePath);
        ensurePartitionedTable(tablePath, hiveTable);

        // partition doesn't have connector property, so check the table
        boolean isHiveTable = isHiveTable(hiveTable.getParameters());

        if (!isHiveTable) {
            throw new CatalogException("Currently only supports partition for hive tables");
        }

        try {
            client.add_partition(instantiateHivePartition(hiveTable, partitionSpec, partition));
        } catch (AlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new PartitionAlreadyExistsException(getName(), tablePath, partitionSpec);
            }
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to create partition %s of table %s", partitionSpec, tablePath),
                    e);
        }
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

        try {
            Table hiveTable = getHiveTable(tablePath);
            client.dropPartition(
                    tablePath.getDatabaseName(),
                    tablePath.getObjectName(),
                    getOrderedFullPartitionValues(
                            partitionSpec, getFieldNames(hiveTable.getPartitionKeys()), tablePath),
                    true);
        } catch (NoSuchObjectException e) {
            if (!ignoreIfNotExists) {
                throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
            }
        } catch (MetaException | TableNotExistException | PartitionSpecInvalidException e) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to drop partition %s of table %s", partitionSpec, tablePath));
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");

        Table hiveTable = getHiveTable(tablePath);

        ensurePartitionedTable(tablePath, hiveTable);

        try {
            // pass -1 as max_parts to fetch all partitions
            return client
                    .listPartitionNames(
                            tablePath.getDatabaseName(), tablePath.getObjectName(), (short) -1)
                    .stream()
                    .map(
                            p ->
                                    HivePartitionUtils.createPartitionSpec(
                                            p,
                                            getHiveConf()
                                                    .getVar(
                                                            HiveConf.ConfVars
                                                                    .DEFAULTPARTITIONNAME)))
                    .collect(Collectors.toList());
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to list partitions of table %s", tablePath), e);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

        Table hiveTable = getHiveTable(tablePath);

        ensurePartitionedTable(tablePath, hiveTable);
        checkValidPartitionSpec(
                partitionSpec, getFieldNames(hiveTable.getPartitionKeys()), tablePath);

        try {
            // partition spec can be partial
            List<String> partialVals =
                    HiveReflectionUtils.getPvals(
                            hiveShim,
                            hiveTable.getPartitionKeys(),
                            partitionSpec.getPartitionSpec());
            return client
                    .listPartitionNames(
                            tablePath.getDatabaseName(),
                            tablePath.getObjectName(),
                            partialVals,
                            (short) -1)
                    .stream()
                    .map(
                            p ->
                                    HivePartitionUtils.createPartitionSpec(
                                            p,
                                            getHiveConf()
                                                    .getVar(
                                                            HiveConf.ConfVars
                                                                    .DEFAULTPARTITIONNAME)))
                    .collect(Collectors.toList());
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to list partitions of table %s", tablePath), e);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> expressions)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        Table hiveTable = getHiveTable(tablePath);
        ensurePartitionedTable(tablePath, hiveTable);
        List<String> partColNames = getFieldNames(hiveTable.getPartitionKeys());
        Optional<String> filter =
                HiveTableUtil.makePartitionFilter(
                        HiveTableUtil.getNonPartitionFields(hiveConf, hiveTable, hiveShim).size(),
                        partColNames,
                        expressions,
                        hiveShim);
        if (!filter.isPresent()) {
            throw new UnsupportedOperationException(
                    "HiveCatalog is unable to handle the partition filter expressions: "
                            + expressions);
        }
        try {
            PartitionSpecProxy partitionSpec =
                    client.listPartitionSpecsByFilter(
                            tablePath.getDatabaseName(),
                            tablePath.getObjectName(),
                            filter.get(),
                            (short) -1);
            List<CatalogPartitionSpec> res = new ArrayList<>(partitionSpec.size());
            PartitionSpecProxy.PartitionIterator partitions = partitionSpec.getPartitionIterator();
            while (partitions.hasNext()) {
                Partition partition = partitions.next();
                Map<String, String> spec = new HashMap<>();
                for (int i = 0; i < partColNames.size(); i++) {
                    spec.put(partColNames.get(i), partition.getValues().get(i));
                }
                res.add(new CatalogPartitionSpec(spec));
            }
            return res;
        } catch (TException e) {
            throw new UnsupportedOperationException(
                    "Failed to list partition by filter from HMS, filter expressions: "
                            + expressions,
                    e);
        }
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

        try {
            Partition hivePartition = getHivePartition(tablePath, partitionSpec);

            Map<String, String> properties = hivePartition.getParameters();

            properties.put(TABLE_LOCATION_URI, hivePartition.getSd().getLocation());

            String comment = properties.remove(HiveCatalogConfig.COMMENT);

            return new CatalogPartitionImpl(properties, comment);
        } catch (NoSuchObjectException
                | MetaException
                | TableNotExistException
                | PartitionSpecInvalidException e) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to get partition %s of table %s", partitionSpec, tablePath),
                    e);
        }
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");
        checkNotNull(newPartition, "New partition cannot be null");

        // Explicitly check if the partition exists or not
        // because alter_partition() doesn't throw NoSuchObjectException like dropPartition() when
        // the target doesn't exist
        try {
            Table hiveTable = getHiveTable(tablePath);
            boolean isHiveTable = isHiveTable(hiveTable.getParameters());
            if (!isHiveTable) {
                throw new CatalogException("Currently only supports partition for hive tables");
            }

            Partition hivePartition = getHivePartition(hiveTable, partitionSpec);
            if (hivePartition == null) {
                if (ignoreIfNotExists) {
                    return;
                }
                throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
            }
            AlterTableOp op = HiveTableUtil.extractAlterTableOp(newPartition.getProperties());
            if (op == null) {
                throw new CatalogException(
                        ALTER_TABLE_OP + " is missing for alter table operation");
            }
            alterTableViaProperties(
                    op,
                    null,
                    null,
                    hivePartition.getParameters(),
                    newPartition.getProperties(),
                    hivePartition.getSd());
            client.alter_partition(
                    tablePath.getDatabaseName(), tablePath.getObjectName(), hivePartition);
        } catch (NoSuchObjectException e) {
            if (!ignoreIfNotExists) {
                throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
            }
        } catch (InvalidOperationException
                | MetaException
                | TableNotExistException
                | PartitionSpecInvalidException e) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to alter existing partition with new partition %s of table %s",
                            partitionSpec, tablePath),
                    e);
        }
    }

    private Partition instantiateHivePartition(
            Table hiveTable, CatalogPartitionSpec partitionSpec, CatalogPartition catalogPartition)
            throws PartitionSpecInvalidException {
        List<String> partCols = getFieldNames(hiveTable.getPartitionKeys());
        List<String> partValues =
                getOrderedFullPartitionValues(
                        partitionSpec,
                        partCols,
                        new ObjectPath(hiveTable.getDbName(), hiveTable.getTableName()));
        // validate partition values
        for (int i = 0; i < partCols.size(); i++) {
            if (isNullOrWhitespaceOnly(partValues.get(i))) {
                throw new PartitionSpecInvalidException(
                        getName(),
                        partCols,
                        new ObjectPath(hiveTable.getDbName(), hiveTable.getTableName()),
                        partitionSpec);
            }
        }
        // TODO: handle GenericCatalogPartition
        StorageDescriptor sd = hiveTable.getSd().deepCopy();
        sd.setLocation(catalogPartition.getProperties().remove(TABLE_LOCATION_URI));

        Map<String, String> properties = new HashMap<>(catalogPartition.getProperties());
        String comment = catalogPartition.getComment();
        if (comment != null) {
            properties.put(HiveCatalogConfig.COMMENT, comment);
        }

        return HiveTableUtil.createHivePartition(
                hiveTable.getDbName(), hiveTable.getTableName(), partValues, sd, properties);
    }

    private void ensurePartitionedTable(ObjectPath tablePath, Table hiveTable)
            throws TableNotPartitionedException {
        if (!isTablePartitioned(hiveTable)) {
            throw new TableNotPartitionedException(getName(), tablePath);
        }
    }

    /** Get field names from field schemas. */
    public static List<String> getFieldNames(List<FieldSchema> fieldSchemas) {
        List<String> names = new ArrayList<>(fieldSchemas.size());
        for (FieldSchema fs : fieldSchemas) {
            names.add(fs.getName());
        }
        return names;
    }

    /**
     * Get a list of ordered partition values by re-arranging them based on the given list of
     * partition keys. If the partition value is null, it'll be converted into default partition
     * name.
     *
     * @param partitionSpec a partition spec.
     * @param partitionKeys a list of partition keys.
     * @param tablePath path of the table to which the partition belongs.
     * @return A list of partition values ordered according to partitionKeys.
     * @throws PartitionSpecInvalidException thrown if partitionSpec and partitionKeys have
     *     different sizes, or any key in partitionKeys doesn't exist in partitionSpec.
     */
    private List<String> getOrderedFullPartitionValues(
            CatalogPartitionSpec partitionSpec, List<String> partitionKeys, ObjectPath tablePath)
            throws PartitionSpecInvalidException {
        Map<String, String> spec = partitionSpec.getPartitionSpec();
        if (spec.size() != partitionKeys.size()) {
            throw new PartitionSpecInvalidException(
                    getName(), partitionKeys, tablePath, partitionSpec);
        }

        List<String> values = new ArrayList<>(spec.size());
        for (String key : partitionKeys) {
            if (!spec.containsKey(key)) {
                throw new PartitionSpecInvalidException(
                        getName(), partitionKeys, tablePath, partitionSpec);
            } else {
                String value = spec.get(key);
                if (value == null) {
                    value = getHiveConf().getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME);
                }
                values.add(value);
            }
        }

        return values;
    }

    /**
     * Check whether a list of partition values are valid based on the given list of partition keys.
     *
     * @param partitionSpec a partition spec.
     * @param partitionKeys a list of partition keys.
     * @param tablePath path of the table to which the partition belongs.
     * @throws PartitionSpecInvalidException thrown if any key in partitionSpec doesn't exist in
     *     partitionKeys.
     */
    private void checkValidPartitionSpec(
            CatalogPartitionSpec partitionSpec, List<String> partitionKeys, ObjectPath tablePath)
            throws PartitionSpecInvalidException {
        for (String key : partitionSpec.getPartitionSpec().keySet()) {
            if (!partitionKeys.contains(key)) {
                throw new PartitionSpecInvalidException(
                        getName(), partitionKeys, tablePath, partitionSpec);
            }
        }
    }

    private Partition getHivePartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, PartitionSpecInvalidException, TException {
        return getHivePartition(getHiveTable(tablePath), partitionSpec);
    }

    @VisibleForTesting
    public Partition getHivePartition(Table hiveTable, CatalogPartitionSpec partitionSpec)
            throws PartitionSpecInvalidException, TException {
        return client.getPartition(
                hiveTable.getDbName(),
                hiveTable.getTableName(),
                getOrderedFullPartitionValues(
                        partitionSpec,
                        getFieldNames(hiveTable.getPartitionKeys()),
                        new ObjectPath(hiveTable.getDbName(), hiveTable.getTableName())));
    }

    // ------ functions ------

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(functionPath, "functionPath cannot be null");
        checkNotNull(function, "function cannot be null");

        Function hiveFunction;
        if (function instanceof CatalogFunctionImpl) {
            hiveFunction = instantiateHiveFunction(functionPath, function);
        } else {
            throw new CatalogException(
                    String.format(
                            "Unsupported catalog function type %s", function.getClass().getName()));
        }

        try {
            client.createFunction(hiveFunction);
        } catch (NoSuchObjectException e) {
            throw new DatabaseNotExistException(getName(), functionPath.getDatabaseName(), e);
        } catch (AlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new FunctionAlreadyExistException(getName(), functionPath, e);
            }
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to create function %s", functionPath.getFullName()), e);
        }
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        checkNotNull(functionPath, "functionPath cannot be null");
        checkNotNull(newFunction, "newFunction cannot be null");

        try {
            // check if function exists
            getFunction(functionPath);

            Function hiveFunction;
            if (newFunction instanceof CatalogFunctionImpl) {
                hiveFunction = instantiateHiveFunction(functionPath, newFunction);
            } else {
                throw new CatalogException(
                        String.format(
                                "Unsupported catalog function type %s",
                                newFunction.getClass().getName()));
            }

            client.alterFunction(
                    functionPath.getDatabaseName(), functionPath.getObjectName(), hiveFunction);
        } catch (FunctionNotExistException e) {
            if (!ignoreIfNotExists) {
                throw e;
            }
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to alter function %s", functionPath.getFullName()), e);
        }
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        checkNotNull(functionPath, "functionPath cannot be null");

        try {
            client.dropFunction(functionPath.getDatabaseName(), functionPath.getObjectName());
        } catch (NoSuchObjectException e) {
            if (!ignoreIfNotExists) {
                throw new FunctionNotExistException(getName(), functionPath, e);
            }
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to drop function %s", functionPath.getFullName()), e);
        }
    }

    @Override
    public List<String> listFunctions(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

        // client.getFunctions() returns empty list when the database doesn't exist
        // thus we need to explicitly check whether the database exists or not
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        try {
            // hive-1.x requires the pattern not being null, so pass a pattern that matches any name
            return client.getFunctions(databaseName, ".*");
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to list functions in database %s", databaseName), e);
        }
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        checkNotNull(functionPath, "functionPath cannot be null or empty");

        try {
            Function function =
                    client.getFunction(
                            functionPath.getDatabaseName(), functionPath.getObjectName());
            List<ResourceUri> resources = new ArrayList<>();
            for (org.apache.hadoop.hive.metastore.api.ResourceUri resourceUri :
                    function.getResourceUris()) {
                switch (resourceUri.getResourceType()) {
                    case JAR:
                        resources.add(
                                new ResourceUri(
                                        org.apache.flink.table.resource.ResourceType.JAR,
                                        resourceUri.getUri()));
                        break;
                    case FILE:
                        resources.add(
                                new ResourceUri(
                                        org.apache.flink.table.resource.ResourceType.FILE,
                                        resourceUri.getUri()));
                        break;
                    case ARCHIVE:
                        resources.add(
                                new ResourceUri(
                                        org.apache.flink.table.resource.ResourceType.ARCHIVE,
                                        resourceUri.getUri()));
                        break;
                    default:
                        throw new CatalogException(
                                String.format(
                                        "Unknown resource type %s for function %s.",
                                        resourceUri.getResourceType(), functionPath.getFullName()));
                }
            }

            if (function.getClassName().startsWith(FLINK_PYTHON_FUNCTION_PREFIX)) {
                return new CatalogFunctionImpl(
                        function.getClassName().substring(FLINK_PYTHON_FUNCTION_PREFIX.length()),
                        FunctionLanguage.PYTHON,
                        resources);
            } else if (function.getClassName().startsWith(FLINK_SCALA_FUNCTION_PREFIX)) {
                return new CatalogFunctionImpl(
                        function.getClassName().substring(FLINK_SCALA_FUNCTION_PREFIX.length()),
                        FunctionLanguage.SCALA,
                        resources);
            } else if (function.getClassName().startsWith("flink:")) {
                // to be compatible with old behavior
                return new CatalogFunctionImpl(
                        function.getClassName().substring("flink:".length()),
                        FunctionLanguage.JAVA,
                        resources);
            } else {
                return new CatalogFunctionImpl(
                        function.getClassName(), FunctionLanguage.JAVA, resources);
            }
        } catch (NoSuchObjectException e) {
            throw new FunctionNotExistException(getName(), functionPath, e);
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to get function %s", functionPath.getFullName()), e);
        }
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        checkNotNull(functionPath, "functionPath cannot be null or empty");

        try {
            return client.getFunction(functionPath.getDatabaseName(), functionPath.getObjectName())
                    != null;
        } catch (NoSuchObjectException e) {
            return false;
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to check whether function %s exists or not",
                            functionPath.getFullName()),
                    e);
        }
    }

    private static Function instantiateHiveFunction(
            ObjectPath functionPath, CatalogFunction function) {
        // Hive Function does not have properties map
        // thus, use a prefix in class name to distinguish Java/Scala and Python functions
        String functionClassName;
        if (function.getFunctionLanguage() == FunctionLanguage.JAVA) {
            functionClassName = function.getClassName();
        } else if (function.getFunctionLanguage() == FunctionLanguage.SCALA) {
            functionClassName = FLINK_SCALA_FUNCTION_PREFIX + function.getClassName();
        } else if (function.getFunctionLanguage() == FunctionLanguage.PYTHON) {
            functionClassName = FLINK_PYTHON_FUNCTION_PREFIX + function.getClassName();
        } else {
            throw new UnsupportedOperationException(
                    "HiveCatalog supports only creating"
                            + " JAVA/SCALA or PYTHON based function for now");
        }

        List<org.apache.hadoop.hive.metastore.api.ResourceUri> resources = new ArrayList<>();
        for (ResourceUri resourceUri : function.getFunctionResources()) {
            switch (resourceUri.getResourceType()) {
                case JAR:
                    resources.add(
                            new org.apache.hadoop.hive.metastore.api.ResourceUri(
                                    ResourceType.JAR, resourceUri.getUri()));
                    break;
                case FILE:
                    resources.add(
                            new org.apache.hadoop.hive.metastore.api.ResourceUri(
                                    ResourceType.FILE, resourceUri.getUri()));
                    break;
                case ARCHIVE:
                    resources.add(
                            new org.apache.hadoop.hive.metastore.api.ResourceUri(
                                    ResourceType.ARCHIVE, resourceUri.getUri()));
                    break;
                default:
                    throw new CatalogException(
                            String.format(
                                    "Unknown resource type %s for function %s.",
                                    resourceUri.getResourceType(), functionPath.getFullName()));
            }
        }

        return new Function(
                // due to https://issues.apache.org/jira/browse/HIVE-22053, we have to normalize
                // function name ourselves
                functionPath.getObjectName().trim().toLowerCase(),
                functionPath.getDatabaseName(),
                functionClassName,
                null, // Owner name
                PrincipalType
                        .GROUP, // Temporarily set to GROUP type because it's required by Hive. May
                // change later
                (int) (System.currentTimeMillis() / 1000),
                FunctionType.JAVA, // FunctionType only has JAVA now
                resources // Resource URIs
                );
    }

    private static boolean isTablePartitioned(Table hiveTable) {
        return hiveTable.getPartitionKeysSize() != 0;
    }

    // ------ stats ------

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            Table hiveTable = getHiveTable(tablePath);
            // the stats we put in table parameters will be overridden by HMS in older Hive
            // versions, so error out
            if (!isTablePartitioned(hiveTable) && hiveVersion.compareTo("1.2.1") < 0) {
                throw new CatalogException(
                        "Alter table stats is not supported in Hive version " + hiveVersion);
            }
            // Set table stats
            if (HiveStatsUtil.statsChanged(tableStatistics, hiveTable.getParameters())) {
                HiveStatsUtil.updateStats(tableStatistics, hiveTable.getParameters());
                client.alter_table(
                        tablePath.getDatabaseName(), tablePath.getObjectName(), hiveTable);
            }
        } catch (TableNotExistException e) {
            if (!ignoreIfNotExists) {
                throw e;
            }
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to alter table stats of table %s", tablePath.getFullName()),
                    e);
        }
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {
        try {
            Table hiveTable = getHiveTable(tablePath);
            // Set table column stats. This only works for non-partitioned tables.
            if (!isTablePartitioned(hiveTable)) {
                client.updateTableColumnStatistics(
                        HiveStatsUtil.createTableColumnStats(
                                hiveTable,
                                columnStatistics.getColumnStatisticsData(),
                                hiveVersion));
            } else {
                throw new TablePartitionedException(getName(), tablePath);
            }
        } catch (TableNotExistException e) {
            if (!ignoreIfNotExists) {
                throw e;
            }
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to alter table column stats of table %s",
                            tablePath.getFullName()),
                    e);
        }
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        try {
            Partition hivePartition = getHivePartition(tablePath, partitionSpec);
            // Set table stats
            if (HiveStatsUtil.statsChanged(partitionStatistics, hivePartition.getParameters())) {
                HiveStatsUtil.updateStats(partitionStatistics, hivePartition.getParameters());
                client.alter_partition(
                        tablePath.getDatabaseName(), tablePath.getObjectName(), hivePartition);
            }
        } catch (TableNotExistException | PartitionSpecInvalidException e) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to alter table stats of table %s 's partition %s",
                            tablePath.getFullName(), String.valueOf(partitionSpec)),
                    e);
        }
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        try {
            Partition hivePartition = getHivePartition(tablePath, partitionSpec);
            Table hiveTable = getHiveTable(tablePath);
            String partName = getEscapedPartitionName(tablePath, partitionSpec, hiveTable);
            client.updatePartitionColumnStatistics(
                    HiveStatsUtil.createPartitionColumnStats(
                            hivePartition,
                            partName,
                            columnStatistics.getColumnStatisticsData(),
                            hiveVersion));
        } catch (TableNotExistException | PartitionSpecInvalidException e) {
            if (!ignoreIfNotExists) {
                throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
            }
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to alter table column stats of table %s 's partition %s",
                            tablePath.getFullName(), String.valueOf(partitionSpec)),
                    e);
        }
    }

    // make a valid partition name that escapes special characters
    private String getEscapedPartitionName(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, Table hiveTable)
            throws PartitionSpecInvalidException {
        List<String> partitionCols = getFieldNames(hiveTable.getPartitionKeys());
        List<String> partitionVals =
                getOrderedFullPartitionValues(partitionSpec, partitionCols, tablePath);
        String defaultPartName = getHiveConf().getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME);
        return FileUtils.makePartName(partitionCols, partitionVals, defaultPartName);
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        Table hiveTable = getHiveTable(tablePath);
        if (!isTablePartitioned(hiveTable)) {
            return HiveStatsUtil.createCatalogTableStatistics(hiveTable.getParameters());
        } else {
            return CatalogTableStatistics.UNKNOWN;
        }
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        Table hiveTable = getHiveTable(tablePath);
        try {
            if (!isTablePartitioned(hiveTable)) {
                List<ColumnStatisticsObj> columnStatisticsObjs =
                        client.getTableColumnStatistics(
                                hiveTable.getDbName(),
                                hiveTable.getTableName(),
                                getFieldNames(hiveTable.getSd().getCols()));
                return new CatalogColumnStatistics(
                        HiveStatsUtil.createCatalogColumnStats(columnStatisticsObjs, hiveVersion));
            } else {
                // TableColumnStats of partitioned table is unknown, the behavior is same as HIVE
                return CatalogColumnStatistics.UNKNOWN;
            }
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to get table column stats of table %s",
                            tablePath.getFullName()),
                    e);
        }
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        try {
            Partition partition = getHivePartition(tablePath, partitionSpec);
            return HiveStatsUtil.createCatalogTableStatistics(partition.getParameters());
        } catch (TableNotExistException | PartitionSpecInvalidException e) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to get partition stats of table %s 's partition %s",
                            tablePath.getFullName(), partitionSpec),
                    e);
        }
    }

    @Override
    public List<CatalogTableStatistics> bulkGetPartitionStatistics(
            ObjectPath tablePath, List<CatalogPartitionSpec> partitionSpecs)
            throws PartitionNotExistException, CatalogException {
        List<Partition> partitions;
        try {
            Table hiveTable = getHiveTable(tablePath);
            partitions = getPartitionsByPartitionSpecs(tablePath, hiveTable, partitionSpecs);
        } catch (TableNotExistException | PartitionSpecInvalidException | TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to get partition stats of table %s 's partitions %s",
                            tablePath.getFullName(), partitionSpecs),
                    e);
        }
        return partitions.stream()
                .map(p -> HiveStatsUtil.createCatalogTableStatistics(p.getParameters()))
                .collect(Collectors.toList());
    }

    private List<Partition> getPartitionsByPartitionSpecs(
            ObjectPath tablePath, Table hiveTable, List<CatalogPartitionSpec> partitionSpecs)
            throws PartitionSpecInvalidException, TException {
        List<Partition> partitions = new ArrayList<>(partitionSpecs.size());
        List<String> partitionNames =
                getPartitionNameByPartitionSpecs(tablePath, hiveTable, partitionSpecs);
        // the order of partitions may be different from the order of partitionSpecs
        partitions.addAll(
                client.getPartitionsByNames(
                        tablePath.getDatabaseName(), tablePath.getObjectName(), partitionNames));
        // make the order partitions be same as the order of partitionSpecs
        Map<String, Partition> partitionMap = new HashMap<>();
        for (Partition partition : partitions) {
            partitionMap.put(
                    FileUtils.makePartName(
                            getFieldNames(hiveTable.getPartitionKeys()),
                            partition.getValues(),
                            getHiveConf().getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME)),
                    partition);
        }
        List<Partition> orderedPartitions = new ArrayList<>(partitions.size());
        for (String partitionName : partitionNames) {
            orderedPartitions.add(partitionMap.get(partitionName));
        }
        return orderedPartitions;
    }

    private List<String> getPartitionNameByPartitionSpecs(
            ObjectPath tablePath, Table hiveTable, List<CatalogPartitionSpec> partitionSpecs)
            throws PartitionSpecInvalidException {
        List<String> partitionsNames = new ArrayList<>(partitionSpecs.size());
        for (CatalogPartitionSpec partSpec : partitionSpecs) {
            partitionsNames.add(getEscapedPartitionName(tablePath, partSpec, hiveTable));
        }
        return partitionsNames;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        try {
            Partition partition = getHivePartition(tablePath, partitionSpec);
            Table hiveTable = getHiveTable(tablePath);
            String partName = getEscapedPartitionName(tablePath, partitionSpec, hiveTable);
            List<String> partNames = new ArrayList<>();
            partNames.add(partName);
            Map<String, List<ColumnStatisticsObj>> partitionColumnStatistics =
                    client.getPartitionColumnStatistics(
                            partition.getDbName(),
                            partition.getTableName(),
                            partNames,
                            getFieldNames(partition.getSd().getCols()));
            List<ColumnStatisticsObj> columnStatisticsObjs =
                    partitionColumnStatistics.get(partName);
            if (columnStatisticsObjs != null && !columnStatisticsObjs.isEmpty()) {
                return new CatalogColumnStatistics(
                        HiveStatsUtil.createCatalogColumnStats(columnStatisticsObjs, hiveVersion));
            } else {
                return CatalogColumnStatistics.UNKNOWN;
            }
        } catch (TableNotExistException | PartitionSpecInvalidException e) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to get table stats of table %s 's partition %s",
                            tablePath.getFullName(), String.valueOf(partitionSpec)),
                    e);
        }
    }

    @Override
    public List<CatalogColumnStatistics> bulkGetPartitionColumnStatistics(
            ObjectPath tablePath, List<CatalogPartitionSpec> partitionSpecs)
            throws PartitionNotExistException, CatalogException {
        Table hiveTable;
        List<String> partitionNames;
        try {
            hiveTable = getHiveTable(tablePath);
            partitionNames = getPartitionNameByPartitionSpecs(tablePath, hiveTable, partitionSpecs);
        } catch (TableNotExistException | PartitionSpecInvalidException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to get partition stats of table %s 's partitions %s",
                            tablePath.getFullName(), partitionSpecs),
                    e);
        }
        List<CatalogColumnStatistics> result = new ArrayList<>(partitionSpecs.size());
        try {
            Map<String, List<ColumnStatisticsObj>> nonPartitionColumnStatisticsObj =
                    client.getPartitionColumnStatistics(
                            tablePath.getDatabaseName(),
                            tablePath.getObjectName(),
                            partitionNames,
                            getFieldNames(hiveTable.getSd().getCols()));
            for (String partitionName : partitionNames) {
                // get statistic for partition columns
                Map<String, CatalogColumnStatisticsDataBase> partitionColumnStatistics =
                        HiveStatsUtil.getCatalogPartitionColumnStats(
                                client,
                                hiveShim,
                                hiveTable,
                                partitionName,
                                hiveTable.getPartitionKeys(),
                                getHiveConf().getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME));

                // get statistic for non-partition columns
                Map<String, CatalogColumnStatisticsDataBase> nonPartitionColumnStatistics =
                        new HashMap<>();
                List<ColumnStatisticsObj> nonColumnStatisticsObjs =
                        nonPartitionColumnStatisticsObj.get(partitionName);
                if (nonColumnStatisticsObjs != null) {
                    nonPartitionColumnStatistics =
                            HiveStatsUtil.createCatalogColumnStats(
                                    nonColumnStatisticsObjs, hiveVersion);
                }
                Map<String, CatalogColumnStatisticsDataBase> allColumnStatistics =
                        new HashMap<>(partitionColumnStatistics);
                allColumnStatistics.putAll(nonPartitionColumnStatistics);
                result.add(new CatalogColumnStatistics(allColumnStatistics));
            }
        } catch (TException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to get table stats of table %s 's partitions %s",
                            tablePath.getFullName(), partitionSpecs),
                    e);
        }

        return result;
    }

    @Override
    public boolean supportsManagedTable() {
        return true;
    }

    @Internal
    public static boolean isHiveTable(Map<String, String> properties) {
        return IDENTIFIER.equalsIgnoreCase(properties.get(CONNECTOR.key()));
    }

    @Internal
    public static boolean isHiveTable(Table table) {
        boolean isHiveTable;
        if (table.getParameters().containsKey(CatalogPropertiesUtil.IS_GENERIC)) {
            isHiveTable =
                    !Boolean.parseBoolean(
                            table.getParameters().remove(CatalogPropertiesUtil.IS_GENERIC));
        } else {
            isHiveTable =
                    !table.getParameters().containsKey(FLINK_PROPERTY_PREFIX + CONNECTOR.key())
                            && !table.getParameters()
                                    .containsKey(FLINK_PROPERTY_PREFIX + CONNECTOR_TYPE);
        }
        return isHiveTable;
    }

    @Internal
    public void loadTable(
            Path loadPath, ObjectPath tablePath, boolean isOverwrite, boolean isSrcLocal) {
        try {
            client.loadTable(loadPath, tablePath.getFullName(), isOverwrite, isSrcLocal);
        } catch (HiveException e) {
            throw new FlinkHiveException("Fail to load table.", e);
        }
    }

    @Internal
    public void loadPartition(
            Path loadPath,
            ObjectPath tablePath,
            Map<String, String> partSpec,
            boolean isOverwrite,
            boolean isSrcLocal) {
        Table hiveTable;
        Map<String, String> orderedPartitionSpec = new LinkedHashMap<>();
        try {
            hiveTable = getHiveTable(tablePath);
        } catch (TableNotExistException e) {
            throw new FlinkHiveException("Fail to get Hive table when try to load partition", e);
        }
        hiveTable
                .getPartitionKeys()
                .forEach(
                        column ->
                                orderedPartitionSpec.put(
                                        column.getName(), partSpec.get(column.getName())));
        client.loadPartition(
                loadPath,
                tablePath.getFullName(),
                orderedPartitionSpec,
                hiveTable.getSd().isStoredAsSubDirectories(),
                isOverwrite,
                isSrcLocal);
    }

    private static void disallowChangeCatalogTableType(
            Map<String, String> existingTableOptions, Map<String, String> newTableOptions) {
        CatalogTableType existingTableType = getCatalogTableType(existingTableOptions);
        CatalogTableType newTableType = getCatalogTableType(newTableOptions);
        checkArgument(
                existingTableType == newTableType,
                String.format(
                        "Changing catalog table type is not allowed. "
                                + "Existing table type is '%s', but new table type is '%s'",
                        existingTableType, newTableType));
    }

    private static CatalogTableType getCatalogTableType(Map<String, String> options) {
        if (isHiveTable(options)) {
            return CatalogTableType.HIVE_TABLE;
        } else if (options.containsKey(CONNECTOR.key()) || options.containsKey(CONNECTOR_TYPE)) {
            return CatalogTableType.FLINK_NON_MANAGED_TABLE;
        }
        return CatalogTableType.FLINK_MANAGED_TABLE;
    }

    private void alterTableViaProperties(
            AlterTableOp alterOp,
            Table hiveTable,
            ResolvedCatalogTable resolvedCatalogTable,
            Map<String, String> oldProps,
            Map<String, String> newProps,
            StorageDescriptor sd) {
        switch (alterOp) {
            case CHANGE_TBL_PROPS:
                oldProps.putAll(newProps);
                break;
            case CHANGE_LOCATION:
                HiveTableUtil.extractLocation(sd, newProps);
                break;
            case CHANGE_FILE_FORMAT:
                String newFileFormat = newProps.remove(STORED_AS_FILE_FORMAT);
                HiveTableUtil.setStorageFormat(sd, newFileFormat, hiveConf);
                break;
            case CHANGE_SERDE_PROPS:
                HiveTableUtil.extractRowFormat(sd, newProps);
                break;
            case ALTER_COLUMNS:
                if (hiveTable == null) {
                    throw new CatalogException("ALTER COLUMNS cannot be done with ALTER PARTITION");
                }

                HiveTableUtil.alterColumns(hiveTable.getSd(), resolvedCatalogTable);
                boolean cascade = Boolean.parseBoolean(newProps.remove(ALTER_COL_CASCADE));
                if (cascade) {
                    if (!isTablePartitioned(hiveTable)) {
                        throw new CatalogException(
                                "ALTER COLUMNS CASCADE for non-partitioned table");
                    }
                    try {
                        for (CatalogPartitionSpec spec :
                                listPartitions(
                                        new ObjectPath(
                                                hiveTable.getDbName(), hiveTable.getTableName()))) {

                            Partition partition = getHivePartition(hiveTable, spec);
                            HiveTableUtil.alterColumns(partition.getSd(), resolvedCatalogTable);
                            client.alter_partition(
                                    hiveTable.getDbName(), hiveTable.getTableName(), partition);
                        }
                    } catch (Exception e) {
                        throw new CatalogException(
                                "Failed to cascade add/replace columns to partitions", e);
                    }
                }
                break;
            default:
                throw new CatalogException("Unsupported alter table operation " + alterOp);
        }
    }

    @Internal
    public static boolean isEmbeddedMetastore(HiveConf hiveConf) {
        return isNullOrWhitespaceOnly(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
    }

    private static Database alterDatabase(Database hiveDB, CatalogDatabase newDatabase) {
        Map<String, String> newParams = newDatabase.getProperties();
        String opStr = newParams.remove(ALTER_DATABASE_OP);
        if (opStr == null) {
            // by default is to alter db properties
            opStr = AlterHiveDatabaseOp.CHANGE_PROPS.name();
        }
        String newLocation = newParams.remove(DATABASE_LOCATION_URI);
        AlterHiveDatabaseOp op = AlterHiveDatabaseOp.valueOf(opStr);
        switch (op) {
            case CHANGE_PROPS:
                hiveDB.setParameters(newParams);
                break;
            case CHANGE_LOCATION:
                hiveDB.setLocationUri(newLocation);
                break;
            case CHANGE_OWNER:
                String ownerName = newParams.remove(DATABASE_OWNER_NAME);
                String ownerType = newParams.remove(DATABASE_OWNER_TYPE);
                hiveDB.setOwnerName(ownerName);
                switch (ownerType) {
                    case ROLE_OWNER:
                        hiveDB.setOwnerType(PrincipalType.ROLE);
                        break;
                    case USER_OWNER:
                        hiveDB.setOwnerType(PrincipalType.USER);
                        break;
                    default:
                        throw new CatalogException("Unsupported database owner type: " + ownerType);
                }
                break;
            default:
                throw new CatalogException("Unsupported alter database op:" + opStr);
        }
        // is_generic is deprecated, remove it
        if (hiveDB.getParameters() != null) {
            hiveDB.getParameters().remove(CatalogPropertiesUtil.IS_GENERIC);
        }
        return hiveDB;
    }

    enum CatalogTableType {
        HIVE_TABLE,
        FLINK_MANAGED_TABLE,
        FLINK_NON_MANAGED_TABLE
    }
}
