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

package org.apache.flink.fs.gshadoop;

import org.apache.flink.fs.gshadoop.writer.GSRecoverableWriterHelper;

/** Abstracts away the implementations of Google Hadoop file system and storage. */
public interface GSFileSystemHelper {
    /**
     * Creates a new instance of the hadoop file system.
     *
     * @return The file system, uninitialized
     */
    org.apache.hadoop.fs.FileSystem createHadoopFileSystem();

    /**
     * Accessor for the associated {@link
     * org.apache.flink.fs.gshadoop.writer.GSRecoverableWriterHelper}, to be used with recoverable
     * writers.
     *
     * @return The recoverable writer helper
     */
    GSRecoverableWriterHelper getRecoverableWriterHelper();
}
