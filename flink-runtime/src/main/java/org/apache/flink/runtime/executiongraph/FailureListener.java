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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.core.plugin.Plugin;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;

/** Failure listener to customize the behavior for each type of failures tracked in job manager. */
public interface FailureListener extends Plugin {

    /**
     * Initialize the listener with JobManagerJobMetricGroup.
     *
     * @param metricGroup metrics group that the listener can add customized metrics definition.
     */
    void init(JobManagerJobMetricGroup metricGroup);

    /**
     * Method to handle each of failures in the listener.
     *
     * @param cause the failure cause
     * @param globalFailure whether the failure is a global failure
     */
    void onFailure(final Throwable cause, boolean globalFailure);
}
