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

package org.apache.flink.connector.hbase.testutil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.source.HBaseSourceOptions;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hbase.thirdparty.com.google.common.io.Closer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Provides static access to a {@link MiniHBaseCluster} for testing. */
public class HBaseTestCluster extends ExternalResource {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseTestCluster.class);

    public static final String COLUMN_FAMILY_BASE = "info";
    public static final String DEFAULT_COLUMN_FAMILY = COLUMN_FAMILY_BASE + 0;
    public static final String QUALIFIER_BASE = "qualifier";
    public static final String DEFAULT_QUALIFIER = QUALIFIER_BASE + 0;

    private MiniHBaseCluster cluster;
    private org.apache.hadoop.conf.Configuration hbaseConf;
    private String testFolder;

    public HBaseTestCluster() {}

    /*
     * How to use it
     */
    public static void main(String[] args) throws Exception {
        HBaseTestCluster hbaseTestCluster = new HBaseTestCluster();
        hbaseTestCluster.startCluster();
        hbaseTestCluster.makeTable("tableName");
        // ...
        hbaseTestCluster.shutdownCluster();
    }

    public void startCluster() throws IOException, InterruptedException, ExecutionException {
        LOG.info("Starting HBase test cluster ...");
        testFolder = Files.createTempDirectory(null).toString();

        // Fallback for windows users with space in user name, will not work if path contains space.
        if (testFolder.contains(" ")) {
            testFolder = "/flink-hbase-test-data/";
        }
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("tempusername"));

        hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("replication.stats.thread.period.seconds", 5);
        hbaseConf.setLong("replication.sleep.before.failover", 2000);
        hbaseConf.setInt("replication.source.maxretriesmultiplier", 10);
        hbaseConf.setBoolean("hbase.replication", true);

        System.setProperty(HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY, testFolder);

        HBaseTestingUtility utility = new HBaseTestingUtility(hbaseConf);
        LOG.info("Testfolder: {}", utility.getDataTestDir().toString());
        try {
            cluster =
                    utility.startMiniCluster(
                            StartMiniClusterOption.builder().numRegionServers(3).build());
            int numRegionServers = utility.getHBaseCluster().getRegionServerThreads().size();
            LOG.info("Number of region servers: {}", numRegionServers);
            LOG.info(
                    "ZooKeeper client address: {}:{}",
                    hbaseConf.get("hbase.zookeeper.quorum"),
                    hbaseConf.get("hbase.zookeeper.property.clientPort"));
            LOG.info(
                    "Master port={}, web UI at port={}",
                    hbaseConf.get("hbase.master.port"),
                    hbaseConf.get("hbase.master.info.port"));

            cluster.waitForActiveAndReadyMaster(30 * 1000);
            HBaseAdmin.available(hbaseConf);
            LOG.info("HBase test cluster up and running ...");

        } catch (Exception e) {
            throw new RuntimeException("Could not start HBase test mini cluster", e);
        }

        assert canConnectToCluster();
    }

    public void shutdownCluster() {
        LOG.info("Shutting down HBase test cluster");
        try {
            try (Closer closer = Closer.create()) {
                // Is executed in reverse order!
                closer.register(Paths.get(testFolder).toFile()::delete);
                closer.register(this::waitForShutDown);
                closer.register(cluster::shutdown);
                closer.register(this::clearReplicationPeers);
                closer.register(this::clearTables);
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to shut down HBase test cluster correctly. Future program state might be compromised.",
                    e);
        }
        LOG.info("HBase test cluster shut down");
    }

    public void waitForShutDown() {
        try {
            CompletableFuture.runAsync(cluster::waitUntilShutDown).get(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while waiting for HBase test cluster to shut down", e);
        } catch (ExecutionException e) {
            e.printStackTrace();
            LOG.error("Exception while waiting for HBase test cluster to shut down", e);
        } catch (TimeoutException e) {
            e.printStackTrace();
            LOG.error("Waiting for HBase test cluster to shut down timed out", e);
        }
    }

    public boolean canConnectToCluster() throws InterruptedException, ExecutionException {
        try {
            return CompletableFuture.supplyAsync(
                            () -> {
                                try (Connection connection =
                                        ConnectionFactory.createConnection(getConfig())) {
                                    return true;
                                } catch (IOException e) {
                                    LOG.error("Error trying to connect to cluster", e);
                                    return false;
                                }
                            })
                    .get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOG.error("Trying to connect to HBase test cluster timed out", e);
            return false;
        }
    }

    public void clearTables() throws IOException {
        try (Admin admin = ConnectionFactory.createConnection(getConfig()).getAdmin()) {
            for (TableDescriptor table : admin.listTableDescriptors()) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
        } catch (IOException e) {
            throw new IOException("Could not clear test cluster tables", e);
        }
    }

    public void clearReplicationPeers() throws IOException {
        try (Admin admin = ConnectionFactory.createConnection(getConfig()).getAdmin()) {
            StringBuilder logMessage = new StringBuilder("Clearing existing replication peers:");
            try (Closer closer = Closer.create()) {
                for (ReplicationPeerDescription desc : admin.listReplicationPeers()) {
                    logMessage.append("\n\t").append(desc.getPeerId()).append(" | ").append(desc);
                    closer.register(() -> admin.removeReplicationPeer(desc.getPeerId()));
                }
                LOG.info(logMessage.toString());
            }
        } catch (IOException e) {
            throw new IOException("Could not clear test cluster replication peers", e);
        }
    }

    public List<ReplicationPeerDescription> getReplicationPeers() throws IOException {
        try (Admin admin = ConnectionFactory.createConnection(getConfig()).getAdmin()) {
            return admin.listReplicationPeers();
        } catch (IOException e) {
            throw new IOException("Error retrieving replication peers", e);
        }
    }

    public void makeTable(String tableName) throws IOException {
        makeTable(tableName, 1);
    }

    /**
     * Creates a table for given name with given number of column families. Column family names
     * start with {@link HBaseTestCluster#COLUMN_FAMILY_BASE} and are indexed, if more than one is
     * requested
     */
    public void makeTable(String tableName, int numColumnFamilies) throws IOException {
        assert numColumnFamilies >= 1;
        try (Admin admin = ConnectionFactory.createConnection(getConfig()).getAdmin()) {
            TableName tableNameObj = TableName.valueOf(tableName);
            if (!admin.tableExists(tableNameObj)) {
                TableDescriptorBuilder tableBuilder =
                        TableDescriptorBuilder.newBuilder(tableNameObj);
                for (int i = 0; i < numColumnFamilies; i++) {
                    ColumnFamilyDescriptorBuilder columnFamilyBuilder =
                            ColumnFamilyDescriptorBuilder.newBuilder(
                                    Bytes.toBytes(COLUMN_FAMILY_BASE + i));
                    columnFamilyBuilder.setScope(1);
                    tableBuilder.setColumnFamily(columnFamilyBuilder.build());
                }
                admin.createTable(tableBuilder.build());
            }
        } catch (IOException e) {
            throw new IOException("Could not create test cluster table", e);
        }
    }

    public void commitPut(String tableName, Put put) throws IOException {
        try (Table htable =
                ConnectionFactory.createConnection(getConfig())
                        .getTable(TableName.valueOf(tableName))) {
            htable.put(put);
            LOG.info("Commited put to row {}", Bytes.toString(put.getRow()));
        } catch (IOException e) {
            throw new IOException("Could not commit put to test cluster", e);
        }
    }

    public String put(String tableName, String value) throws IOException {
        return put(tableName, 1, value);
    }

    public void delete(String tableName, String rowKey, String columnFamily, String qualifier)
            throws IOException {
        try (Table htable =
                ConnectionFactory.createConnection(getConfig())
                        .getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(rowKey.getBytes());
            delete.addColumn(columnFamily.getBytes(), qualifier.getBytes());
            htable.delete(delete);
            LOG.info("Deleted row {}", rowKey);
        } catch (IOException e) {
            throw new IOException("Could not delete row in test cluster", e);
        }
    }

    public String put(String tableName, int numColumnFamilies, String... values)
            throws IOException {
        assert numColumnFamilies >= 1;
        assert values.length >= numColumnFamilies;
        try (Table htable =
                ConnectionFactory.createConnection(getConfig())
                        .getTable(TableName.valueOf(tableName))) {

            String rowKey = UUID.randomUUID().toString();
            Put put = new Put(rowKey.getBytes());
            int index = 0;
            for (int columnFamily = 0; columnFamily < numColumnFamilies; columnFamily++) {
                int columnQualifier = 0;
                for (;
                        index + columnQualifier
                                < values.length * (columnFamily + 1) / numColumnFamilies;
                        columnQualifier++) {
                    put.addColumn(
                            (COLUMN_FAMILY_BASE + columnFamily).getBytes(),
                            (QUALIFIER_BASE + columnQualifier).getBytes(),
                            values[index + columnQualifier].getBytes());
                }
                index += columnQualifier;
            }
            htable.put(put);
            LOG.info("Added row " + rowKey);
            return rowKey;
        } catch (IOException e) {
            throw new IOException("Could not put to test cluster", e);
        }
    }

    public org.apache.hadoop.conf.Configuration getConfig() {
        return hbaseConf;
    }

    public Configuration getConfigurationForTable(String tableName) {
        Configuration config = new Configuration();
        config.setString(HBaseSourceOptions.TABLE_NAME, tableName);
        return config;
    }

    @Override
    protected void before() throws Throwable {
        startCluster();
    }

    @Override
    protected void after() {
        shutdownCluster();
    }
}
