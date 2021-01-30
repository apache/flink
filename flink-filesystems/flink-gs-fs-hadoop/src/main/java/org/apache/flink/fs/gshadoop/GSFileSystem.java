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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.gshadoop.writer.GSRecoverableOptions;
import org.apache.flink.fs.gshadoop.writer.GSRecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Implementation of the Flink {@link org.apache.flink.core.fs.FileSystem} interface for Google
 * Storage.
 */
public class GSFileSystem extends HadoopFileSystem {

    @VisibleForTesting final org.apache.hadoop.fs.FileSystem hadoopFileSystem;

    @VisibleForTesting final GSRecoverableOptions options;

    /**
     * Creates a GSFileSystem based on the given Hadoop GS file system. The given Hadoop file system
     * object is expected to be initialized already.
     *
     * @param hadoopFileSystem The hadoop filesystem
     * @param options The {@link org.apache.flink.fs.gshadoop.writer.GSRecoverableOptions} for this
     *     file system instance.
     */
    public GSFileSystem(
            org.apache.hadoop.fs.FileSystem hadoopFileSystem, GSRecoverableOptions options) {
        super(hadoopFileSystem);
        this.hadoopFileSystem = Preconditions.checkNotNull(hadoopFileSystem);
        this.options = Preconditions.checkNotNull(options);
    }

    @Override
    public RecoverableWriter createRecoverableWriter() throws IOException {
        return new GSRecoverableWriter(options);
    }
}
