/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.utils;

import org.apache.flink.runtime.jobmaster.JobResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** Testing utils surrounding the {@link JobResult}. */
public final class JobResultUtils {
    public static void assertSuccess(final JobResult result) {
        throwAssertionErrorOnFailedResult(result);
    }

    public static void assertIncomplete(final CompletableFuture<JobResult> result) {
        if (result.isDone()) {
            final JobResult jobResult;
            try {
                jobResult = result.get();
            } catch (InterruptedException | ExecutionException e) {
                // we already know it is done so this doesn't happen
                throw new AssertionError(
                        "Unexpected exception when processing finished future.", e);
            }

            if (jobResult.isSuccess()) {
                throw new AssertionError("Job finished successfully.");
            } else {
                throwAssertionErrorOnFailedResult(jobResult);
            }
        }
    }

    private static void throwAssertionErrorOnFailedResult(final JobResult result) {
        if (!result.isSuccess()) {
            if (result.getSerializedThrowable().isPresent()) {
                throw new AssertionError(
                        "Job failed.",
                        result.getSerializedThrowable()
                                .get()
                                .deserializeError(JobResultUtils.class.getClassLoader()));
            } else {
                throw new AssertionError("Job was not successful but did not fail with an error.");
            }
        }
    }
}
