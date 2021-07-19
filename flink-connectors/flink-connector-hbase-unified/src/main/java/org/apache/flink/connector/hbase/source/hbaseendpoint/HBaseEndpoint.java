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

package org.apache.flink.connector.hbase.source.hbaseendpoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.hbase.source.HBaseSourceOptions;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceEvent;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService.BlockingInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Endpoint for HBase CDC via HBase replication. Runs an rpc server that is registered as
 * replication peer at HBase. A table is specified in the constructor, and the column families to
 * replicate are specified when the replication is started with {@link #startReplication}. The
 * incoming WAL edits are internally buffered and can be retrieved with {@link #getAll}.
 */
@Internal
public class HBaseEndpoint implements ReplicationTargetInterface {

    private static final AdminProtos.ReplicateWALEntryResponse REPLICATE_WAL_ENTRY_RESPONSE =
            AdminProtos.ReplicateWALEntryResponse.newBuilder().build();

    private static final Logger LOG = LoggerFactory.getLogger(HBaseEndpoint.class);
    private final String clusterKey;
    /**
     * The id under which the replication target is made known to the source cluster. Using the same
     * id as a previous consumer - that has not been unregistered - prevents that already
     * acknowledged events are sent again.
     */
    private final String replicationPeerId;

    private final org.apache.hadoop.conf.Configuration hbaseConf;
    private final RecoverableZooKeeper zooKeeper;
    private final RpcServer rpcServer;
    private final FutureCompletingBlockingQueue<HBaseSourceEvent> walEdits;
    private final String hostName;
    private final String tableName;
    private boolean isRunning = false;

    public HBaseEndpoint(
            org.apache.hadoop.conf.Configuration hbaseConf, Configuration sourceConfiguration)
            throws IOException, InterruptedException {
        this.hbaseConf = hbaseConf;
        this.replicationPeerId = UUID.randomUUID().toString().substring(0, 5);
        this.clusterKey = replicationPeerId + "_clusterKey";
        this.hostName = sourceConfiguration.get(HBaseSourceOptions.HOST_NAME);
        this.tableName = sourceConfiguration.get(HBaseSourceOptions.TABLE_NAME);
        int queueCapacity = sourceConfiguration.get(HBaseSourceOptions.ENDPOINT_QUEUE_CAPACITY);
        this.walEdits = new FutureCompletingBlockingQueue<>(queueCapacity);

        // Setup
        rpcServer = createServer();
        zooKeeper = connectToZooKeeper();
        registerAtZooKeeper();
    }

    private RecoverableZooKeeper connectToZooKeeper() throws IOException {
        // Using private ZKUtil and RecoverableZooKeeper here, because the stability is so much
        // improved
        RecoverableZooKeeper zooKeeper =
                ZKUtil.connect(hbaseConf, getZookeeperClientAddress(), null);
        LOG.debug("Connected to Zookeeper");
        return zooKeeper;
    }

    private RpcServer createServer() throws IOException {
        Server server = new EmptyHBaseServer(hbaseConf);
        InetSocketAddress initialIsa = new InetSocketAddress(hostName, 0);
        String name = "regionserver/" + initialIsa.toString();

        RpcServer.BlockingServiceAndInterface bsai =
                new RpcServer.BlockingServiceAndInterface(
                        AdminProtos.AdminService.newReflectiveBlockingService(this),
                        BlockingInterface.class);
        RpcServer rpcServer =
                RpcServerFactory.createRpcServer(
                        server,
                        name,
                        Arrays.asList(bsai),
                        initialIsa,
                        hbaseConf,
                        new FifoRpcScheduler(
                                hbaseConf,
                                hbaseConf.getInt("hbase.regionserver.handler.count", 10)));

        rpcServer.start();
        LOG.debug("Started rpc server at {}", initialIsa);
        return rpcServer;
    }

    private void registerAtZooKeeper() throws InterruptedException {
        createZKPath(getZookeeperRootNode() + "/" + clusterKey, null, CreateMode.PERSISTENT);
        createZKPath(
                getZookeeperRootNode() + "/" + clusterKey + "/rs", null, CreateMode.PERSISTENT);

        UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(clusterKey));
        createZKPath(
                getZookeeperRootNode() + "/" + clusterKey + "/hbaseid",
                Bytes.toBytes(uuid.toString()),
                CreateMode.PERSISTENT);

        ServerName serverName =
                ServerName.valueOf(
                        hostName,
                        rpcServer.getListenerAddress().getPort(),
                        System.currentTimeMillis());
        createZKPath(
                getZookeeperRootNode() + "/" + clusterKey + "/rs/" + serverName.getServerName(),
                null,
                CreateMode.EPHEMERAL);

        LOG.debug("Registered rpc server node at zookeeper");
    }

    /**
     * Blocks as long as the queue is empty. If the queue isn't empty it will try and get as many
     * elements as possible from the queue. It is not guaranteed that at least one element is in the
     * returned list.
     *
     * @return a list of {@link HBaseSourceEvent}.
     */
    public List<HBaseSourceEvent> getAll() {
        if (!isRunning) {
            // Protects from infinite waiting
            throw new RuntimeException("Consumer is not running");
        }
        List<HBaseSourceEvent> elements = new ArrayList<>();
        HBaseSourceEvent event;

        try {
            walEdits.getAvailabilityFuture().get();
            while ((event = walEdits.poll()) != null) {
                elements.add(event);
            }
            return elements;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Can't retrieve elements from queue", e);
        }
    }

    public void wakeup() {
        walEdits.notifyAvailable();
    }

    public void close() throws InterruptedException {
        isRunning = false;

        try (Connection connection = ConnectionFactory.createConnection(hbaseConf);
                Admin admin = connection.getAdmin()) {
            admin.removeReplicationPeer(replicationPeerId);
        } catch (IOException e) {
            LOG.error(
                    "Error unregistering HBase endpoint replication peer. Will proceed with shutdown, but source cluster might be dirty.",
                    e);
        }

        try {
            org.apache.zookeeper.ZKUtil.deleteRecursive(
                    zooKeeper.getZooKeeper(), getZookeeperRootNode() + "/" + clusterKey);
        } catch (KeeperException e) {
            LOG.error(
                    "Error unregistering up HBase endpoint from zookeeper. Will proceed with shutdown, but source cluster might be dirty.",
                    e);
        }

        try {
            zooKeeper.close();
            LOG.debug("Closed connection to ZooKeeper");
        } finally {
            rpcServer.stop();
            LOG.debug("Closed HBase replication target rpc server");
        }
    }

    public void startReplication(List<String> columnFamilies) throws IOException {
        if (isRunning) {
            throw new RuntimeException("HBase replication endpoint is already running");
        }
        try (Connection connection = ConnectionFactory.createConnection(hbaseConf);
                Admin admin = connection.getAdmin()) {

            ReplicationPeerConfig peerConfig = createPeerConfig(this.tableName, columnFamilies);
            if (admin.listReplicationPeers().stream()
                    .map(ReplicationPeerDescription::getPeerId)
                    .anyMatch(replicationPeerId::equals)) {
                admin.updateReplicationPeerConfig(replicationPeerId, peerConfig);
            } else {
                admin.addReplicationPeer(replicationPeerId, peerConfig);
            }
            isRunning = true;
        }
    }

    @Override
    public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(
            RpcController controller, AdminProtos.ReplicateWALEntryRequest request)
            throws ServiceException {
        List<AdminProtos.WALEntry> entries = request.getEntryList();
        CellScanner cellScanner = ((HBaseRpcController) controller).cellScanner();

        try {
            for (final AdminProtos.WALEntry entry : entries) {
                final String table =
                        TableName.valueOf(entry.getKey().getTableName().toByteArray()).toString();
                final int count = entry.getAssociatedCellCount();
                for (int i = 0; i < count; i++) {
                    if (!cellScanner.advance()) {
                        throw new ArrayIndexOutOfBoundsException(
                                "Expected WAL entry to have "
                                        + count
                                        + "elements, but cell scanner did not have cell for index"
                                        + i);
                    }

                    Cell cell = cellScanner.current();
                    HBaseSourceEvent event = HBaseSourceEvent.fromCell(table, cell, i);
                    walEdits.put(0, event);
                }
            }
        } catch (Exception e) {
            throw new ServiceException("Could not replicate WAL entry in HBase endpoint", e);
        }

        return REPLICATE_WAL_ENTRY_RESPONSE;
    }

    private ReplicationPeerConfig createPeerConfig(String table, List<String> columnFamilies) {
        Map<TableName, List<String>> tableColumnFamiliesMap = new HashMap<>();
        tableColumnFamiliesMap.put(TableName.valueOf(table), columnFamilies);
        return ReplicationPeerConfig.newBuilder()
                .setClusterKey(
                        getZookeeperClientAddress()
                                + ":"
                                + getZookeeperRootNode()
                                + "/"
                                + clusterKey)
                .setReplicateAllUserTables(false)
                .setTableCFsMap(tableColumnFamiliesMap)
                .build();
    }

    private String getZookeeperClientAddress() {
        return hbaseConf.get("hbase.zookeeper.quorum")
                + ":"
                + hbaseConf.get("hbase.zookeeper.property.clientPort");
    }

    private String getZookeeperRootNode() {
        return hbaseConf.get("zookeeper.znode.parent", "/hbase");
    }

    private void createZKPath(final String path, byte[] data, CreateMode createMode)
            throws InterruptedException {
        try {
            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
            }
        } catch (KeeperException e) {
            throw new RuntimeException("Error creating ZK path in HBase replication endpoint", e);
        }
    }
}
