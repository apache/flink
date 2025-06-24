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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Public;

/**
 * A {@code LocatedFileStatus} is a {@link FileStatus} that contains additionally the location
 * information of the file directly. The information is accessible through the {@link
 * #getBlockLocations()} method.
 *
 * <p>This class eagerly communicates the block information (including locations) when that
 * information is readily (or cheaply) available. That way users can avoid an additional call to
 * {@link FileSystem#getFileBlockLocations(FileStatus, long, long)}, which is an additional RPC call
 * for each file.
 */
@Public
public interface LocatedFileStatus extends FileStatus {

    /**
     * Gets the location information for the file. The location is per block, because each block may
     * live potentially at a different location.
     *
     * <p>Files without location information typically expose one block with no host information for
     * that block.
     */
    BlockLocation[] getBlockLocations();
}
