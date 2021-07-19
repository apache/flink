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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Minimal implementation of {@link Server} to enable the creation of {@link RpcServer} via {@link
 * RpcServerFactory#createRpcServer(Server, String, List, InetSocketAddress, Configuration,
 * RpcScheduler)}.
 */
@Internal
public class EmptyHBaseServer implements Server {

    private final Configuration configuration;

    public EmptyHBaseServer(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void abort(String why, Throwable e) {
        throw new UnsupportedOperationException(
                "Operation \"abort\" not implemented in class " + getClass());
    }

    @Override
    public boolean isAborted() {
        throw new UnsupportedOperationException(
                "Operation \"isAborted\" not implemented in class " + getClass());
    }

    @Override
    public void stop(String why) {
        throw new UnsupportedOperationException(
                "Operation \"stop\" not implemented in class " + getClass());
    }

    @Override
    public boolean isStopped() {
        throw new UnsupportedOperationException(
                "Operation \"isStopped\" not implemented in class " + getClass());
    }

    @Override
    public ZKWatcher getZooKeeper() {
        throw new UnsupportedOperationException(
                "Operation \"getZooKeeper\" not implemented in class " + getClass());
    }

    @Override
    public Connection getConnection() {
        throw new UnsupportedOperationException(
                "Operation \"getConnection\" not implemented in class " + getClass());
    }

    @Override
    public Connection createConnection(Configuration conf) {
        throw new UnsupportedOperationException(
                "Operation \"createConnection\" not implemented in class " + getClass());
    }

    @Override
    public ClusterConnection getClusterConnection() {
        throw new UnsupportedOperationException(
                "Operation \"getClusterConnection\" not implemented in class " + getClass());
    }

    @Override
    public ServerName getServerName() {
        throw new UnsupportedOperationException(
                "Operation \"getServerName\" not implemented in class " + getClass());
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
        throw new UnsupportedOperationException(
                "Operation \"getCoordinatedStateManager\" not implemented in class " + getClass());
    }

    @Override
    public ChoreService getChoreService() {
        throw new UnsupportedOperationException(
                "Operation \"getChoreService\" not implemented in class " + getClass());
    }
}
