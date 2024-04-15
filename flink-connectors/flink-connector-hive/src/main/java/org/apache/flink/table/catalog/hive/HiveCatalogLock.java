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

import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.connectors.hive.util.HiveConfUtils;
import org.apache.flink.connectors.hive.util.JobConfUtils;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.util.TimeUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.Callable;

import static org.apache.flink.table.catalog.hive.HiveConfOptions.LOCK_ACQUIRE_TIMEOUT;
import static org.apache.flink.table.catalog.hive.HiveConfOptions.LOCK_CHECK_MAX_SLEEP;

/**
 * Hive {@link CatalogLock}.
 *
 * @deprecated This class will be removed soon. Please see FLIP-346 for more details.
 */
@Deprecated
public class HiveCatalogLock implements CatalogLock {

    private final HiveMetastoreClientWrapper client;
    private final long checkMaxSleep;
    private final long acquireTimeout;

    public HiveCatalogLock(
            HiveMetastoreClientWrapper client, long checkMaxSleep, long acquireTimeout) {
        this.client = client;
        this.checkMaxSleep = checkMaxSleep;
        this.acquireTimeout = acquireTimeout;
    }

    @Override
    public <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception {
        long lockId = lock(database, table);
        try {
            return callable.call();
        } finally {
            unlock(lockId);
        }
    }

    private long lock(String database, String table)
            throws UnknownHostException, TException, InterruptedException {
        final LockComponent lockComponent =
                new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, database);
        lockComponent.setTablename(table);
        lockComponent.unsetOperationType();
        final LockRequest lockRequest =
                new LockRequest(
                        Collections.singletonList(lockComponent),
                        System.getProperty("user.name"),
                        InetAddress.getLocalHost().getHostName());
        LockResponse lockResponse = this.client.lock(lockRequest);

        long nextSleep = 50;
        long startRetry = System.currentTimeMillis();
        while (lockResponse.getState() == LockState.WAITING) {
            nextSleep *= 2;
            if (nextSleep > checkMaxSleep) {
                nextSleep = checkMaxSleep;
            }
            Thread.sleep(nextSleep);

            lockResponse = client.checkLock(lockResponse.getLockid());
            if (System.currentTimeMillis() - startRetry > acquireTimeout) {
                break;
            }
        }
        long retryDuration = System.currentTimeMillis() - startRetry;

        if (lockResponse.getState() != LockState.ACQUIRED) {
            if (lockResponse.getState() == LockState.WAITING) {
                client.unlock(lockResponse.getLockid());
            }
            throw new RuntimeException(
                    "Acquire lock failed with time: " + Duration.ofMillis(retryDuration));
        }
        return lockResponse.getLockid();
    }

    private void unlock(long lockId) throws TException {
        client.unlock(lockId);
    }

    @Override
    public void close() {
        this.client.close();
    }

    /** Create a hive lock factory. */
    public static CatalogLock.Factory createFactory(HiveConf hiveConf) {
        return new HiveCatalogLockFactory(hiveConf);
    }

    @Deprecated
    private static class HiveCatalogLockFactory implements CatalogLock.Factory {

        private static final long serialVersionUID = 1L;

        private final JobConfWrapper confWrapper;

        public HiveCatalogLockFactory(HiveConf hiveConf) {
            this.confWrapper =
                    new JobConfWrapper(JobConfUtils.createJobConfWithCredentials(hiveConf));
        }

        @Override
        public CatalogLock create() {
            JobConf conf = confWrapper.conf();
            String version = conf.get(HiveCatalogFactoryOptions.HIVE_VERSION.key());
            long checkMaxSleep =
                    TimeUtils.parseDuration(
                                    conf.get(
                                            LOCK_CHECK_MAX_SLEEP.key(),
                                            TimeUtils.getStringInMillis(
                                                    LOCK_CHECK_MAX_SLEEP.defaultValue())))
                            .toMillis();
            long acquireTimeout =
                    TimeUtils.parseDuration(
                                    conf.get(
                                            LOCK_ACQUIRE_TIMEOUT.key(),
                                            TimeUtils.getStringInMillis(
                                                    LOCK_ACQUIRE_TIMEOUT.defaultValue())))
                            .toMillis();
            return new HiveCatalogLock(
                    HiveMetastoreClientFactory.create(HiveConfUtils.create(conf), version),
                    checkMaxSleep,
                    acquireTimeout);
        }
    }
}
