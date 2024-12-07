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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;

import java.io.File;
import java.io.IOException;

/** A blob store doing nothing. */
public class VoidBlobStore implements BlobStoreService {

    @Override
    public boolean put(File localFile, JobID jobId, BlobKey blobKey) throws IOException {
        return false;
    }

    @Override
    public boolean get(JobID jobId, BlobKey blobKey, File localFile) throws IOException {
        return false;
    }

    @Override
    public boolean delete(JobID jobId, BlobKey blobKey) {
        return true;
    }

    @Override
    public boolean deleteAll(JobID jobId) {
        return true;
    }

    @Override
    public void cleanupAllData() {}

    @Override
    public void close() throws IOException {}
}
