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

package org.apache.flink.runtime.rest.handler.job.rescales;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.rest.handler.RestHandlerException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** An exception that is thrown if the requested job rescales related data unavailable. */
public class RescalesUnavailableException extends RestHandlerException {

    private static final long serialVersionUID = 1L;

    public RescalesUnavailableException(String message) {
        super(message, HttpResponseStatus.NOT_FOUND, RestHandlerException.LoggingBehavior.IGNORE);
    }

    public static RescalesUnavailableException createForJob(JobID jobId) {
        return new RescalesUnavailableException(
                String.format(
                        "The job `%s` has not enabled the `%s` scheduler, or it has been enabled but the value of configuration option `%s` has not been set to greater than `0`.",
                        jobId,
                        JobManagerOptions.SchedulerType.Adaptive,
                        WebOptions.MAX_ADAPTIVE_SCHEDULER_RESCALE_HISTORY_SIZE.key()));
    }
}
