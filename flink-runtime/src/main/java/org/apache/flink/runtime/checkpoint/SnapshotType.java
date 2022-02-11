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

import java.io.Serializable;

/** The type of checkpoint to perform. */
public interface SnapshotType extends Serializable {

    boolean isSavepoint();

    String getName();

    SharingFilesStrategy getSharingFilesStrategy();

    /** Defines what files can be shared across snapshots. */
    enum SharingFilesStrategy {
        // current snapshot can share files with previous snapshots.
        // new snapshots can use files of the current snapshot
        FORWARD_BACKWARD,
        // later snapshots can share files with the current snapshot
        FORWARD,
        // current snapshot can not use files of older ones, future snapshots can
        // not use files of the current one.
        NO_SHARING;
    }
}
