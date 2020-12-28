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

package org.apache.flink.runtime.persistence;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.FlinkException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Class which stores state via the provided {@link RetrievableStateStorageHelper} and writes the
 * returned state handle to distributed coordination system(e.g. Zookeeper, Kubernetes, etc.).
 *
 * <p>To avoid concurrent modification issues, the implementation needs to ensure that only the
 * leader could update the state store.
 *
 * <p>Even some methods name contains the "lock"(e.g. {@link #getAndLock}), it does not mean the
 * implementation has to actually lock a specific state handle. Also it could have an empty
 * implementation for release operation.
 *
 * @param <T> Type of state
 * @param <R> Type of {@link ResourceVersion}
 */
public interface StateHandleStore<T extends Serializable, R extends ResourceVersion<R>> {

    /**
     * Persist the state to distributed storage(e.g. S3, HDFS, etc.). And then creates a state
     * handle, stores it in the distributed coordination system(e.g. ZooKeeper, Kubernetes, etc.).
     *
     * @param name Key name in ConfigMap or child path name in ZooKeeper
     * @param state State to be added
     * @throws AlreadyExistException if the name already exists
     * @throws Exception if persisting state or writing state handle failed
     */
    RetrievableStateHandle<T> addAndLock(String name, T state) throws Exception;

    /**
     * Replaces a state handle in the distributed coordination system and discards the old state
     * handle.
     *
     * @param name Key name in ConfigMap or child path name in ZooKeeper
     * @param resourceVersion resource version of previous storage object. If the resource version
     *     does not match, the replace operation will fail. Since there is an unexpected update
     *     operation snuck in.
     * @param state State to be replace with
     * @throws NotExistException if the name does not exist
     * @throws Exception if persisting state or writing state handle failed
     */
    void replace(String name, R resourceVersion, T state) throws Exception;

    /**
     * Returns resource version of state handle with specific name on the underlying storage.
     *
     * @param name Key name in ConfigMap or child path name in ZooKeeper
     * @return current resource version on the underlying storage.
     * @throws Exception if the check existence operation failed
     */
    R exists(String name) throws Exception;

    /**
     * Gets the {@link RetrievableStateHandle} stored with the given name.
     *
     * @param name Key name in ConfigMap or child path name in ZooKeeper
     * @return The retrieved state handle
     * @throws IOException if the method failed to deserialize the stored state handle
     * @throws NotExistException when the name does not exist
     * @throws Exception if get state handle failed
     */
    RetrievableStateHandle<T> getAndLock(String name) throws Exception;

    /**
     * Gets all available state handles from the storage.
     *
     * @return All retrieved state handles.
     * @throws Exception if get state handle operation failed
     */
    List<Tuple2<RetrievableStateHandle<T>, String>> getAllAndLock() throws Exception;

    /**
     * Return a list of all valid name for state handles.
     *
     * @return List of valid state handle name. The name is key name in ConfigMap or child path name
     *     in ZooKeeper.
     * @throws Exception if get handle operation failed
     */
    Collection<String> getAllHandles() throws Exception;

    /**
     * Releases the lock for the given state handle and tries to remove the state handle if it is no
     * longer locked. It returns the {@link RetrievableStateHandle} stored under the given state
     * node if any. Also the state on the external storage will be discarded.
     *
     * @param name Key name in ConfigMap or child path name in ZooKeeper
     * @return True if the state handle could be removed.
     * @throws Exception if releasing, removing the handles or discarding the state failed
     */
    boolean releaseAndTryRemove(String name) throws Exception;

    /**
     * Releases and removes all the states. Not only the state handles in the distributed
     * coordination system will be removed, but also the real state data on the distributed storage
     * will be discarded.
     *
     * @throws Exception if releasing, removing the handles or discarding the state failed
     */
    void releaseAndTryRemoveAll() throws Exception;

    /**
     * Only clears all the state handle pointers on Kubernetes or ZooKeeper.
     *
     * @throws Exception if removing the handles failed
     */
    void clearEntries() throws Exception;

    /**
     * Releases the lock on the specific state handle so that it could be deleted by other {@link
     * StateHandleStore}. If no lock exists or the underlying storage does not support, nothing will
     * happen.
     *
     * @throws Exception if releasing the lock
     */
    void release(String name) throws Exception;

    /**
     * Releases all the locks on corresponding state handle so that it could be deleted by other
     * {@link StateHandleStore}. If no lock exists or the underlying storage does not support,
     * nothing will happen.
     *
     * @throws Exception if releasing the locks
     */
    void releaseAll() throws Exception;

    /** The key does not exist in ConfigMap or the Zookeeper node does not exists. */
    class NotExistException extends FlinkException {

        private static final long serialVersionUID = 1L;

        /**
         * Creates a new Exception with the given message and cause.
         *
         * @param message The exception message
         * @param cause The cause exception
         */
        public NotExistException(String message, Throwable cause) {
            super(message, cause);
        }

        /**
         * Creates a new Exception with the given message and null cause.
         *
         * @param message The exception message
         */
        public NotExistException(String message) {
            super(message);
        }
    }

    /** The key already exists in ConfigMap or the Zookeeper node already exists. */
    class AlreadyExistException extends FlinkException {

        private static final long serialVersionUID = 1L;

        /**
         * Creates a new Exception with the given message and cause.
         *
         * @param message The exception message
         * @param cause The cause exception
         */
        public AlreadyExistException(String message, Throwable cause) {
            super(message, cause);
        }

        /**
         * Creates a new Exception with the given message and null cause.
         *
         * @param message The exception message
         */
        public AlreadyExistException(String message) {
            super(message);
        }
    }
}
