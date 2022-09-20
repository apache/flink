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

package org.apache.flink.table.catalog.hive.client;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Wrapper class for Hive Metastore Client, which embeds a HiveShim layer to handle different Hive
 * versions. Methods provided mostly conforms to IMetaStoreClient interfaces except those that
 * require shims.
 */
@Internal
public class HiveMetastoreClientWrapper implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreClientWrapper.class);

    private final HiveConf hiveConf;
    private final HiveShim hiveShim;
    private volatile Hive hive;

    public HiveMetastoreClientWrapper(HiveConf hiveConf, String hiveVersion) {
        this(hiveConf, HiveShimLoader.loadHiveShim(hiveVersion));
    }

    public HiveMetastoreClientWrapper(HiveConf hiveConf, HiveShim hiveShim) {
        this.hiveConf = Preconditions.checkNotNull(hiveConf, "HiveConf cannot be null");
        this.hiveShim = hiveShim;
    }

    @Override
    public void close() {
        getClient().close();
    }

    public List<String> getDatabases(String pattern) throws MetaException, TException {
        return getClient().getDatabases(pattern);
    }

    public List<String> getAllDatabases() throws MetaException, TException {
        return getClient().getAllDatabases();
    }

    public List<String> getAllTables(String databaseName)
            throws MetaException, TException, UnknownDBException {
        return getClient().getAllTables(databaseName);
    }

    public void dropTable(String databaseName, String tableName)
            throws MetaException, TException, NoSuchObjectException {
        getClient().dropTable(databaseName, tableName);
    }

    public void dropTable(
            String dbName, String tableName, boolean deleteData, boolean ignoreUnknownTable)
            throws MetaException, NoSuchObjectException, TException {
        getClient().dropTable(dbName, tableName, deleteData, ignoreUnknownTable);
    }

    public boolean tableExists(String databaseName, String tableName)
            throws MetaException, TException, UnknownDBException {
        return getClient().tableExists(databaseName, tableName);
    }

    public Database getDatabase(String name)
            throws NoSuchObjectException, MetaException, TException {
        return getClient().getDatabase(name);
    }

    public Table getTable(String databaseName, String tableName)
            throws MetaException, NoSuchObjectException, TException {
        return getClient().getTable(databaseName, tableName);
    }

    public Partition add_partition(Partition partition)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return getClient().add_partition(partition);
    }

    public int add_partitions(List<Partition> partitionList)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return getClient().add_partitions(partitionList);
    }

    public Partition getPartition(String databaseName, String tableName, List<String> list)
            throws NoSuchObjectException, MetaException, TException {
        return getClient().getPartition(databaseName, tableName, list);
    }

    public List<Partition> getPartitionsByNames(
            String databaseName, String tableName, List<String> partitionNames) throws TException {
        return getClient().getPartitionsByNames(databaseName, tableName, partitionNames);
    }

    public List<String> listPartitionNames(
            String databaseName, String tableName, short maxPartitions)
            throws MetaException, TException {
        return getClient().listPartitionNames(databaseName, tableName, maxPartitions);
    }

    public List<String> listPartitionNames(
            String databaseName,
            String tableName,
            List<String> partitionValues,
            short maxPartitions)
            throws MetaException, TException, NoSuchObjectException {
        return getClient()
                .listPartitionNames(databaseName, tableName, partitionValues, maxPartitions);
    }

    public void createTable(Table table)
            throws AlreadyExistsException, InvalidObjectException, MetaException,
                    NoSuchObjectException, TException {
        getClient().createTable(table);
    }

    public void createDatabase(Database database)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        getClient().createDatabase(database);
    }

    public void dropDatabase(String name, boolean deleteData, boolean ignoreIfNotExists)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        getClient().dropDatabase(name, deleteData, ignoreIfNotExists);
    }

    public void dropDatabase(
            String name, boolean deleteData, boolean ignoreIfNotExists, boolean cascade)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        getClient().dropDatabase(name, deleteData, ignoreIfNotExists, cascade);
    }

    public void alterDatabase(String name, Database database)
            throws NoSuchObjectException, MetaException, TException {
        getClient().alterDatabase(name, database);
    }

    public boolean dropPartition(
            String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        return getClient().dropPartition(databaseName, tableName, partitionValues, deleteData);
    }

    public void renamePartition(
            String databaseName,
            String tableName,
            List<String> partitionValues,
            Partition partition)
            throws InvalidOperationException, MetaException, TException {
        getClient().renamePartition(databaseName, tableName, partitionValues, partition);
    }

    public void createFunction(Function function)
            throws InvalidObjectException, MetaException, TException {
        getClient().createFunction(function);
    }

    public void alterFunction(String databaseName, String functionName, Function function)
            throws InvalidObjectException, MetaException, TException {
        getClient().alterFunction(databaseName, functionName, function);
    }

    public void dropFunction(String databaseName, String functionName)
            throws MetaException, NoSuchObjectException, InvalidObjectException,
                    InvalidInputException, TException {
        getClient().dropFunction(databaseName, functionName);
    }

    public List<String> getFunctions(String databaseName, String pattern)
            throws MetaException, TException {
        return getClient().getFunctions(databaseName, pattern);
    }

    public List<ColumnStatisticsObj> getTableColumnStatistics(
            String databaseName, String tableName, List<String> columnNames)
            throws NoSuchObjectException, MetaException, TException {
        return getClient().getTableColumnStatistics(databaseName, tableName, columnNames);
    }

    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName, String tableName, List<String> partNames, List<String> colNames)
            throws NoSuchObjectException, MetaException, TException {
        return getClient().getPartitionColumnStatistics(dbName, tableName, partNames, colNames);
    }

    public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
                    InvalidInputException {
        return getClient().updateTableColumnStatistics(columnStatistics);
    }

    public boolean updatePartitionColumnStatistics(ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
                    InvalidInputException {
        return getClient().updatePartitionColumnStatistics(columnStatistics);
    }

    public List<Partition> listPartitions(
            String dbName, String tblName, List<String> partVals, short max) throws TException {
        return getClient().listPartitions(dbName, tblName, partVals, max);
    }

    public List<Partition> listPartitions(String dbName, String tblName, short max)
            throws TException {
        return getClient().listPartitions(dbName, tblName, max);
    }

    public PartitionSpecProxy listPartitionSpecsByFilter(
            String dbName, String tblName, String filter, short max) throws TException {
        return getClient().listPartitionSpecsByFilter(dbName, tblName, filter, max);
    }

    // -------- Start of shimmed methods ----------

    public Set<String> getNotNullColumns(Configuration conf, String dbName, String tableName) {
        return hiveShim.getNotNullColumns(getClient(), conf, dbName, tableName);
    }

    public Optional<UniqueConstraint> getPrimaryKey(String dbName, String tableName, byte trait) {
        return hiveShim.getPrimaryKey(getClient(), dbName, tableName, trait);
    }

    public List<String> getViews(String databaseName) throws UnknownDBException, TException {
        return hiveShim.getViews(getClient(), databaseName);
    }

    public Function getFunction(String databaseName, String functionName)
            throws MetaException, TException {
        try {
            // Hive may not throw NoSuchObjectException if function doesn't exist, instead it throws
            // a MetaException
            return getClient().getFunction(databaseName, functionName);
        } catch (MetaException e) {
            // need to check the cause and message of this MetaException to decide whether it should
            // actually be a NoSuchObjectException
            if (e.getCause() instanceof NoSuchObjectException) {
                throw (NoSuchObjectException) e.getCause();
            }
            if (e.getMessage().startsWith(NoSuchObjectException.class.getSimpleName())) {
                throw new NoSuchObjectException(e.getMessage());
            }
            throw e;
        }
    }

    public void alter_table(String databaseName, String tableName, Table table)
            throws InvalidOperationException, MetaException, TException {
        hiveShim.alterTable(getClient(), databaseName, tableName, table);
    }

    public void alter_partition(String databaseName, String tableName, Partition partition)
            throws InvalidOperationException, MetaException, TException {
        hiveShim.alterPartition(getClient(), databaseName, tableName, partition);
    }

    public void createTableWithConstraints(
            Table table,
            Configuration conf,
            UniqueConstraint pk,
            List<Byte> pkTraits,
            List<String> notNullCols,
            List<Byte> nnTraits) {
        hiveShim.createTableWithConstraints(
                getClient(), table, conf, pk, pkTraits, notNullCols, nnTraits);
    }

    public LockResponse checkLock(long lockid)
            throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
        return getClient().checkLock(lockid);
    }

    public LockResponse lock(LockRequest request)
            throws NoSuchTxnException, TxnAbortedException, TException {
        return getClient().lock(request);
    }

    public void unlock(long lockid) throws NoSuchLockException, TxnOpenException, TException {
        getClient().unlock(lockid);
    }

    public void loadTable(Path loadPath, String tableName, boolean replace, boolean isSrcLocal)
            throws HiveException {
        initHive();
        hiveShim.loadTable(hive, loadPath, tableName, replace, isSrcLocal);
    }

    public void loadPartition(
            Path loadPath,
            String tableName,
            Map<String, String> partSpec,
            boolean isSkewedStoreAsSubdir,
            boolean replace,
            boolean isSrcLocal) {
        initHive();
        hiveShim.loadPartition(
                hive, loadPath, tableName, partSpec, isSkewedStoreAsSubdir, replace, isSrcLocal);
    }

    private void initHive() {
        if (this.hive == null) {
            synchronized (this) {
                if (this.hive == null) {
                    try {
                        this.hive = Hive.get(hiveConf);
                    } catch (HiveException e) {
                        throw new FlinkHiveException(e);
                    }
                }
            }
        }
    }

    private void updateHive() {
        synchronized (this) {
            try {
                hive = Hive.get(hiveConf);
            } catch (HiveException e) {
                throw new FlinkHiveException(e);
            }
        }
    }

    private IMetaStoreClient getClient() {
        try {
            updateHive();
            return hive.getMSC(HiveCatalog.isEmbeddedMetastore(hiveConf), false);
        } catch (MetaException e) {
            throw new FlinkHiveException("Cannot get hive metastore client.", e);
        }
    }
}
