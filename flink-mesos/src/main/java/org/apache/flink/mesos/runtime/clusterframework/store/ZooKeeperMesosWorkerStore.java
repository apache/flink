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

package org.apache.flink.mesos.runtime.clusterframework.store;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.ZooKeeperSharedCount;
import org.apache.flink.runtime.zookeeper.ZooKeeperSharedValue;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.runtime.zookeeper.ZooKeeperVersionedValue;
import org.apache.flink.util.FlinkException;

import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import scala.Option;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A ZooKeeper-backed Mesos worker store.
 *
 * @deprecated Apache Mesos support was deprecated in Flink 1.13 and is subject to removal in the
 *     future (see FLINK-22352 for further details).
 */
@Deprecated
public class ZooKeeperMesosWorkerStore implements MesosWorkerStore {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperMesosWorkerStore.class);

    private final Object startStopLock = new Object();

    /** Flag indicating whether this instance is running. */
    private boolean isRunning;

    /** A persistent value of the assigned framework ID. */
    private final ZooKeeperSharedValue frameworkIdInZooKeeper;

    /** A persistent count of all tasks created, for generating unique IDs. */
    private final ZooKeeperSharedCount totalTaskCountInZooKeeper;

    /** A persistent store of serialized workers. */
    private final ZooKeeperStateHandleStore<MesosWorkerStore.Worker> workersInZooKeeper;

    @SuppressWarnings("unchecked")
    public ZooKeeperMesosWorkerStore(
            ZooKeeperStateHandleStore<MesosWorkerStore.Worker> workersInZooKeeper,
            ZooKeeperSharedValue frameworkIdInZooKeeper,
            ZooKeeperSharedCount totalTaskCountInZooKeeper)
            throws Exception {
        this.workersInZooKeeper = checkNotNull(workersInZooKeeper, "workersInZooKeeper");
        this.frameworkIdInZooKeeper =
                checkNotNull(frameworkIdInZooKeeper, "frameworkIdInZooKeeper");
        this.totalTaskCountInZooKeeper =
                checkNotNull(totalTaskCountInZooKeeper, "totalTaskCountInZooKeeper");
    }

    @Override
    public void start() throws Exception {
        synchronized (startStopLock) {
            if (!isRunning) {
                isRunning = true;
                frameworkIdInZooKeeper.start();
                totalTaskCountInZooKeeper.start();
            }
        }
    }

    public void stop(boolean cleanup) throws Exception {
        synchronized (startStopLock) {
            if (isRunning) {
                frameworkIdInZooKeeper.close();
                totalTaskCountInZooKeeper.close();

                if (cleanup) {
                    workersInZooKeeper.releaseAndTryRemoveAll();
                }

                isRunning = false;
            }
        }
    }

    /** Verifies that the state is running. */
    private void verifyIsRunning() {
        checkState(isRunning, "Not running. Forgot to call start()?");
    }

    /**
     * Get the persisted framework ID.
     *
     * @return the current ID or empty if none is yet persisted.
     * @throws Exception on ZK failures, interruptions.
     */
    @Override
    public Option<Protos.FrameworkID> getFrameworkID() throws Exception {
        synchronized (startStopLock) {
            verifyIsRunning();

            Option<Protos.FrameworkID> frameworkID;
            byte[] value = frameworkIdInZooKeeper.getValue();
            if (value.length == 0) {
                frameworkID = Option.empty();
            } else {
                frameworkID =
                        Option.apply(
                                Protos.FrameworkID.newBuilder()
                                        .setValue(
                                                new String(value, ConfigConstants.DEFAULT_CHARSET))
                                        .build());
            }

            return frameworkID;
        }
    }

    /**
     * Update the persisted framework ID.
     *
     * @param frameworkID the new ID or empty to remove the persisted ID.
     * @throws Exception on ZK failures, interruptions.
     */
    @Override
    public void setFrameworkID(Option<Protos.FrameworkID> frameworkID) throws Exception {
        synchronized (startStopLock) {
            verifyIsRunning();

            byte[] value =
                    frameworkID.isDefined()
                            ? frameworkID.get().getValue().getBytes(ConfigConstants.DEFAULT_CHARSET)
                            : new byte[0];
            frameworkIdInZooKeeper.setValue(value);
        }
    }

    /** Generates a new task ID. */
    @Override
    public Protos.TaskID newTaskID() throws Exception {
        synchronized (startStopLock) {
            verifyIsRunning();

            int nextCount;
            boolean success;
            do {
                ZooKeeperVersionedValue<Integer> count =
                        totalTaskCountInZooKeeper.getVersionedValue();
                nextCount = count.getValue() + 1;
                success = totalTaskCountInZooKeeper.trySetCount(count, nextCount);
            } while (!success);

            Protos.TaskID taskID =
                    Protos.TaskID.newBuilder().setValue(TASKID_FORMAT.format(nextCount)).build();
            return taskID;
        }
    }

    @Override
    public List<MesosWorkerStore.Worker> recoverWorkers() throws Exception {
        synchronized (startStopLock) {
            verifyIsRunning();

            List<Tuple2<RetrievableStateHandle<Worker>, String>> handles =
                    workersInZooKeeper.getAllAndLock();

            if (handles.isEmpty()) {
                return Collections.emptyList();
            } else {
                List<MesosWorkerStore.Worker> workers = new ArrayList<>(handles.size());

                for (Tuple2<RetrievableStateHandle<Worker>, String> handle : handles) {
                    final Worker worker;

                    try {
                        worker = handle.f0.retrieveState();
                    } catch (ClassNotFoundException cnfe) {
                        throw new FlinkException(
                                "Could not retrieve Mesos worker from state handle under "
                                        + handle.f1
                                        + ". This indicates that you are trying to recover from state written by an "
                                        + "older Flink version which is not compatible. Try cleaning the state handle store.",
                                cnfe);
                    } catch (IOException ioe) {
                        throw new FlinkException(
                                "Could not retrieve Mesos worker from state handle under "
                                        + handle.f1
                                        + ". This indicates that the retrieved state handle is broken. Try cleaning "
                                        + "the state handle store.",
                                ioe);
                    }

                    workers.add(worker);
                }

                return workers;
            }
        }
    }

    @Override
    public void putWorker(MesosWorkerStore.Worker worker) throws Exception {

        checkNotNull(worker, "worker");
        String path = getPathForWorker(worker.taskID());

        synchronized (startStopLock) {
            verifyIsRunning();

            final IntegerResourceVersion currentVersion = workersInZooKeeper.exists(path);
            if (!currentVersion.isExisting()) {
                workersInZooKeeper.addAndLock(path, worker);
                LOG.debug("Added {} in ZooKeeper.", worker);
            } else {
                workersInZooKeeper.replace(path, currentVersion, worker);
                LOG.debug("Updated {} in ZooKeeper.", worker);
            }
        }
    }

    @Override
    public boolean removeWorker(Protos.TaskID taskID) throws Exception {
        checkNotNull(taskID, "taskID");
        String path = getPathForWorker(taskID);
        synchronized (startStopLock) {
            verifyIsRunning();

            if (!workersInZooKeeper.exists(path).isExisting()) {
                LOG.debug("No such worker {} in ZooKeeper.", taskID);
                return false;
            }

            workersInZooKeeper.releaseAndTryRemove(path);
            LOG.debug("Removed worker {} from ZooKeeper.", taskID);
            return true;
        }
    }

    /** Get the ZK path for the given task ID (with leading slash). */
    private static String getPathForWorker(Protos.TaskID taskID) {
        checkNotNull(taskID, "taskID");
        return String.format("/%s", taskID.getValue());
    }
}
