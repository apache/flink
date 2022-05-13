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
import org.apache.flink.util.function.TriFunctionWithException;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Function;

/** {@link BlobStore} implementation for testing purposes. */
public class TestingBlobStore implements BlobStore {

    @Nonnull
    private final TriFunctionWithException<File, JobID, BlobKey, Boolean, IOException> putFunction;

    @Nonnull private final BiFunction<JobID, BlobKey, Boolean> deleteFunction;

    @Nonnull private final Function<JobID, Boolean> deleteAllFunction;

    @Nonnull
    private final TriFunctionWithException<JobID, BlobKey, File, Boolean, IOException> getFunction;

    public TestingBlobStore(
            @Nonnull
                    TriFunctionWithException<File, JobID, BlobKey, Boolean, IOException>
                            putFunction,
            @Nonnull BiFunction<JobID, BlobKey, Boolean> deleteFunction,
            @Nonnull Function<JobID, Boolean> deleteAllFunction,
            @Nonnull
                    TriFunctionWithException<JobID, BlobKey, File, Boolean, IOException>
                            getFunction) {
        this.putFunction = putFunction;
        this.deleteFunction = deleteFunction;
        this.deleteAllFunction = deleteAllFunction;
        this.getFunction = getFunction;
    }

    @Override
    public boolean put(File localFile, JobID jobId, BlobKey blobKey) throws IOException {
        return putFunction.apply(localFile, jobId, blobKey);
    }

    @Override
    public boolean delete(JobID jobId, BlobKey blobKey) {
        return deleteFunction.apply(jobId, blobKey);
    }

    @Override
    public boolean deleteAll(JobID jobId) {
        return deleteAllFunction.apply(jobId);
    }

    @Override
    public boolean get(JobID jobId, BlobKey blobKey, File localFile) throws IOException {
        return getFunction.apply(jobId, blobKey, localFile);
    }
}
