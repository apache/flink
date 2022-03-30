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

package org.apache.flink.runtime.scheduler.stopwithsavepoint;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.throwable.ThrowableAnnotation;
import org.apache.flink.runtime.throwable.ThrowableType;
import org.apache.flink.util.FlinkException;

/**
 * Exception thrown when a savepoint has been created successfully when stopping with savepoint, but
 * the job has not finished. In that case side-effects might have not been committed. This exception
 * is used to communicate that to the use.
 */
@Experimental
@ThrowableAnnotation(ThrowableType.NonRecoverableError)
public class StopWithSavepointStoppingException extends FlinkException {
    private final String savepointPath;

    public StopWithSavepointStoppingException(String savepointPath, JobID jobID) {
        super(formatMessage(savepointPath, jobID));
        this.savepointPath = savepointPath;
    }

    public StopWithSavepointStoppingException(String savepointPath, JobID jobID, Throwable cause) {
        super(formatMessage(savepointPath, jobID), cause);
        this.savepointPath = savepointPath;
    }

    private static String formatMessage(String savepointPath, JobID jobID) {
        return String.format(
                "A savepoint has been created at: %s, but the corresponding job %s failed "
                        + "during stopping. The savepoint is consistent, but might have "
                        + "uncommitted transactions. If you want to commit the transaction "
                        + "please restart a job from this savepoint.",
                savepointPath, jobID);
    }

    public String getSavepointPath() {
        return savepointPath;
    }
}
