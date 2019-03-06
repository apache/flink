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

package org.apache.flink.fs.gcs.common;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.gcs.common.writer.GcsRecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;

public class FlinkGcsFileSystem extends HadoopFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkGcsFileSystem.class);

    private final Storage storage;

    public FlinkGcsFileSystem(FileSystem hadoopFileSystem) throws IOException {
        super(hadoopFileSystem);

        this.storage = StorageOptions
                .newBuilder()
                .setCredentials(GoogleCredentials.fromStream(new
                        FileInputStream("/Users/fdriesprong/Downloads/bigdata-evaluation-37ca861e9307.json")))
                .build()
                .getService();
    }

    @Override
    public RecoverableWriter createRecoverableWriter() {
        return new GcsRecoverableWriter(this.storage);
    }

}
