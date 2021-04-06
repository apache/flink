/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;

/**
 * An interface exposing the information concerning the current (open) part file that is necessary
 * to the {@link RollingPolicy} in order to determine if it should roll the part file or not.
 */
@PublicEvolving
public interface PartFileInfo<BucketID> {

    /**
     * @return The bucket identifier of the current buffer, as returned by the {@link
     *     BucketAssigner#getBucketId(Object, BucketAssigner.Context)}.
     */
    BucketID getBucketId();

    /** @return The creation time (in ms) of the currently open part file. */
    long getCreationTime();

    /** @return The size of the currently open part file. */
    long getSize() throws IOException;

    /** @return The last time (in ms) the currently open part file was written to. */
    long getLastUpdateTime();
}
