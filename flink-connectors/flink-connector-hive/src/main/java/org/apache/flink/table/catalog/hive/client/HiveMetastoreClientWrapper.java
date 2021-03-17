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
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Wrapper class for Hive Metastore Client, which embeds a HiveShim layer to handle different Hive
 * versions. Methods provided mostly conforms to IMetaStoreClient interfaces except those that
 * require shims.
 */
@Internal
public class HiveMetastoreClientWrapper implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreClientWrapper.class);

    private final IMetaStoreClient client;
    private final HiveConf hiveConf;
    private final HiveShim hiveShim;

    public HiveMetastoreClientWrapper(HiveConf hiveConf, String hiveVersion) {
        this.hiveConf = Preconditions.checkNotNull(hiveConf, "HiveConf cannot be null");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(hiveVersion),
                "hiveVersion cannot be null or empty");
        hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
        // use synchronized client in case we're talking to a remote HMS
        client =
                HiveCatalog.isEmbeddedMetastore(hiveConf)
                        ? createMetastoreClient()
                        : HiveMetaStoreClient.newSynchronizedClient(createMetastoreClient());
    }

    @Override
    public void close() {
        client.close();
    }

    public List<String> getDatabases(String pattern) throws MetaException, TException {
        return client.getDatabases(pattern);
    }

    public List<String> getAllDatabases() throws MetaException, TException {
        return client.getAllDatabases();
    }

    public List<String> getAllTables(String databaseName)
            throws MetaException, TException, UnknownDBException {
        return client.getAllTables(databaseName);
    }

    public void dropTable(String databaseName, String tableName)
            throws MetaException, TException, NoSuchObjectException {
        client.dropTable(databaseName, tableName);
    }

    public void dropTable(
            String dbName, String tableName, boolean deleteData, boolean ignoreUnknownTable)
            throws MetaException, NoSuchObjectException, TException {
        client.dropTable(dbName, tableName, deleteData, ignoreUnknownTable);
    }

    public boolean tableExists(String databaseName, String tableName)
            throws MetaException, TException, UnknownDBException {
        return client.tableExists(databaseName, tableName);
    }

    public Database getDatabase(String name)
            throws NoSuchObjectException, MetaException, TException {
        return client.getDatabase(name);
    }

    public Table getTable(String databaseName, String tableName)
            throws MetaException, NoSuchObjectException, TException {
        return client.getTable(databaseName, tableName);
    }

    public Partition add_partition(Partition partition)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return client.add_partition(partition);
    }

    public int add_partitions(List<Partition> partitionList)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return client.add_partitions(partitionList);
    }

    public Partition getPartition(String databaseName, String tableName, List<String> list)
            throws NoSuchObjectException, MetaException, TException {
        return client.getPartition(databaseName, tableName, list);
    }

    public List<String> listPartitionNames(
            String databaseName, String tableName, short maxPartitions)
            throws MetaException, TException {
        return client.listPartitionNames(databaseName, tableName, maxPartitions);
    }

    public List<String> listPartitionNames(
            String databaseName,
            String tableName,
            List<String> partitionValues,
            short maxPartitions)
            throws MetaException, TException, NoSuchObjectException {
        return client.listPartitionNames(databaseName, tableName, partitionValues, maxPartitions);
    }

    public void createTable(Table table)
            throws AlreadyExistsException, InvalidObjectException, MetaException,
                    NoSuchObjectException, TException {
        client.createTable(table);
    }

    public void createDatabase(Database database)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        client.createDatabase(database);
    }

    public void dropDatabase(String name, boolean deleteData, boolean ignoreIfNotExists)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        client.dropDatabase(name, deleteData, ignoreIfNotExists);
    }

    public void dropDatabase(
            String name, boolean deleteData, boolean ignoreIfNotExists, boolean cascade)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        client.dropDatabase(name, deleteData, ignoreIfNotExists, cascade);
    }

    public void alterDatabase(String name, Database database)
            throws NoSuchObjectException, MetaException, TException {
        client.alterDatabase(name, database);
    }

    public boolean dropPartition(
            String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        return client.dropPartition(databaseName, tableName, partitionValues, deleteData);
    }

    public void renamePartition(
            String databaseName,
            String tableName,
            List<String> partitionValues,
            Partition partition)
            throws InvalidOperationException, MetaException, TException {
        client.renamePartition(databaseName, tableName, partitionValues, partition);
    }

    public void createFunction(Function function)
            throws InvalidObjectException, MetaException, TException {
        client.createFunction(function);
    }

    public void alterFunction(String databaseName, String functionName, Function function)
            throws InvalidObjectException, MetaException, TException {
        client.alterFunction(databaseName, functionName, function);
    }

    public void dropFunction(String databaseName, String functionName)
            throws MetaException, NoSuchObjectException, InvalidObjectException,
                    InvalidInputException, TException {
        client.dropFunction(databaseName, functionName);
    }

    public List<String> getFunctions(String databaseName, String pattern)
            throws MetaException, TException {
        return client.getFunctions(databaseName, pattern);
    }

    public List<ColumnStatisticsObj> getTableColumnStatistics(
            String databaseName, String tableName, List<String> columnNames)
            throws NoSuchObjectException, MetaException, TException {
        return client.getTableColumnStatistics(databaseName, tableName, columnNames);
    }

    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName, String tableName, List<String> partNames, List<String> colNames)
            throws NoSuchObjectException, MetaException, TException {
        return client.getPartitionColumnStatistics(dbName, tableName, partNames, colNames);
    }

    public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
                    InvalidInputException {
        return client.updateTableColumnStatistics(columnStatistics);
    }

    public boolean updatePartitionColumnStatistics(ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
                    InvalidInputException {
        return client.updatePartitionColumnStatistics(columnStatistics);
    }

    public List<Partition> listPartitions(
            String dbName, String tblName, List<String> partVals, short max) throws TException {
        return client.listPartitions(dbName, tblName, partVals, max);
    }

    public List<Partition> listPartitions(String dbName, String tblName, short max)
            throws TException {
        return client.listPartitions(dbName, tblName, max);
    }

    public PartitionSpecProxy listPartitionSpecsByFilter(
            String dbName, String tblName, String filter, short max) throws TException {
        return client.listPartitionSpecsByFilter(dbName, tblName, filter, max);
    }

    // -------- Start of shimmed methods ----------

    public Set<String> getNotNullColumns(Configuration conf, String dbName, String tableName) {
        return hiveShim.getNotNullColumns(client, conf, dbName, tableName);
    }

    public Optional<UniqueConstraint> getPrimaryKey(String dbName, String tableName, byte trait) {
        return hiveShim.getPrimaryKey(client, dbName, tableName, trait);
    }

    public List<String> getViews(String databaseName) throws UnknownDBException, TException {
        return hiveShim.getViews(client, databaseName);
    }

    private IMetaStoreClient createMetastoreClient() {
        return hiveShim.getHiveMetastoreClient(hiveConf);
    }

    public Function getFunction(String databaseName, String functionName)
            throws MetaException, TException {
        try {
            // Hive may not throw NoSuchObjectException if function doesn't exist, instead it throws
            // a MetaException
            return client.getFunction(databaseName, functionName);
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
        hiveShim.alterTable(client, databaseName, tableName, table);
    }

    public void alter_partition(String databaseName, String tableName, Partition partition)
            throws InvalidOperationException, MetaException, TException {
        hiveShim.alterPartition(client, databaseName, tableName, partition);
    }

    public void createTableWithConstraints(
            Table table,
            Configuration conf,
            UniqueConstraint pk,
            List<Byte> pkTraits,
            List<String> notNullCols,
            List<Byte> nnTraits) {
        hiveShim.createTableWithConstraints(
                client, table, conf, pk, pkTraits, notNullCols, nnTraits);
    }
}
