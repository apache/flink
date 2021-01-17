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

package org.apache.flink.runtime.checkpoint;

/**
 * {@link CompletedCheckpointStore} utility interfaces. For example, convert a name(e.g. ZooKeeper
 * path, key name in Kubernetes ConfigMap) to checkpoint id in {@link Long} format, or vice versa.
 */
public interface CheckpointStoreUtil {

    long INVALID_CHECKPOINT_ID = -1L;

    /**
     * Get the name in external storage from checkpoint id.
     *
     * @param checkpointId checkpoint id
     * @return Key name in ConfigMap or child path name in ZooKeeper
     */
    String checkpointIDToName(long checkpointId);

    /**
     * Get the checkpoint id from name.
     *
     * @param name Key name in ConfigMap or child path name in ZooKeeper
     * @return parsed checkpoint id. Or {@link #INVALID_CHECKPOINT_ID} when parsing failed.
     */
    long nameToCheckpointID(String name);
}
