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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.util.function.TriFunctionWithException;

import java.io.File;
import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Builder for the {@link TestingBlobStoreBuilder}. */
public class TestingBlobStoreBuilder {
    private static final TriFunctionWithException<File, JobID, BlobKey, Boolean, IOException>
            DEFAULT_PUT_FUNCTION = (ignoredA, ignoredB, ignoredC) -> true;
    private static final BiFunction<JobID, BlobKey, Boolean> DEFAULT_DELETE_FUNCTION =
            (ignoredA, ignoredB) -> true;
    private static final Function<JobID, Boolean> DEFAULT_DELETE_ALL_FUNCTION = ignored -> true;
    private static final TriFunctionWithException<JobID, BlobKey, File, Boolean, IOException>
            DEFAULT_GET_FUNCTION = (ignoredA, ignoredB, ignoredC) -> true;
    private static final TriFunctionWithException<
                    File, ApplicationID, BlobKey, Boolean, IOException>
            DEFAULT_PUT_FOR_APPLICATION_FUNCTION = (ignoredA, ignoredB, ignoredC) -> true;
    private static final BiFunction<ApplicationID, BlobKey, Boolean>
            DEFAULT_DELETE_FOR_APPLICATION_FUNCTION = (ignoredA, ignoredB) -> true;
    private static final Function<ApplicationID, Boolean>
            DEFAULT_DELETE_ALL_FOR_APPLICATION_FUNCTION = ignored -> true;
    private static final TriFunctionWithException<
                    ApplicationID, BlobKey, File, Boolean, IOException>
            DEFAULT_GET_FOR_APPLICATION_FUNCTION = (ignoredA, ignoredB, ignoredC) -> true;

    private TriFunctionWithException<File, JobID, BlobKey, Boolean, IOException> putFunction =
            DEFAULT_PUT_FUNCTION;
    private BiFunction<JobID, BlobKey, Boolean> deleteFunction = DEFAULT_DELETE_FUNCTION;
    private Function<JobID, Boolean> deleteAllFunction = DEFAULT_DELETE_ALL_FUNCTION;
    private TriFunctionWithException<JobID, BlobKey, File, Boolean, IOException> getFunction =
            DEFAULT_GET_FUNCTION;
    private TriFunctionWithException<File, ApplicationID, BlobKey, Boolean, IOException>
            putForApplicationFunction = DEFAULT_PUT_FOR_APPLICATION_FUNCTION;
    private BiFunction<ApplicationID, BlobKey, Boolean> deleteForApplicationFunction =
            DEFAULT_DELETE_FOR_APPLICATION_FUNCTION;
    private Function<ApplicationID, Boolean> deleteAllForApplicationFunction =
            DEFAULT_DELETE_ALL_FOR_APPLICATION_FUNCTION;
    private TriFunctionWithException<ApplicationID, BlobKey, File, Boolean, IOException>
            getForApplicationFunction = DEFAULT_GET_FOR_APPLICATION_FUNCTION;

    public TestingBlobStoreBuilder setPutFunction(
            TriFunctionWithException<File, JobID, BlobKey, Boolean, IOException> putFunction) {
        this.putFunction = putFunction;
        return this;
    }

    public TestingBlobStoreBuilder setDeleteFunction(
            BiFunction<JobID, BlobKey, Boolean> deleteFunction) {
        this.deleteFunction = deleteFunction;
        return this;
    }

    public TestingBlobStoreBuilder setDeleteAllFunction(
            Function<JobID, Boolean> deleteAllFunction) {
        this.deleteAllFunction = deleteAllFunction;
        return this;
    }

    public TestingBlobStoreBuilder setGetFunction(
            TriFunctionWithException<JobID, BlobKey, File, Boolean, IOException> getFunction) {
        this.getFunction = getFunction;
        return this;
    }

    public TestingBlobStoreBuilder setPutForApplicationFunction(
            TriFunctionWithException<File, ApplicationID, BlobKey, Boolean, IOException>
                    putForApplicationFunction) {
        this.putForApplicationFunction = putForApplicationFunction;
        return this;
    }

    public TestingBlobStoreBuilder setDeleteForApplicationFunction(
            BiFunction<ApplicationID, BlobKey, Boolean> deleteForApplicationFunction) {
        this.deleteForApplicationFunction = deleteForApplicationFunction;
        return this;
    }

    public TestingBlobStoreBuilder setDeleteAllForApplicationFunction(
            Function<ApplicationID, Boolean> deleteAllForApplicationFunction) {
        this.deleteAllForApplicationFunction = deleteAllForApplicationFunction;
        return this;
    }

    public TestingBlobStoreBuilder setGetForApplicationFunction(
            TriFunctionWithException<ApplicationID, BlobKey, File, Boolean, IOException>
                    getForApplicationFunction) {
        this.getForApplicationFunction = getForApplicationFunction;
        return this;
    }

    public TestingBlobStore createTestingBlobStore() {
        return new TestingBlobStore(
                putFunction,
                deleteFunction,
                deleteAllFunction,
                getFunction,
                putForApplicationFunction,
                deleteForApplicationFunction,
                deleteAllForApplicationFunction,
                getForApplicationFunction);
    }
}
