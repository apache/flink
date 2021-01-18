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

package org.apache.flink.runtime.failurelistener;

import org.apache.flink.core.failurelistener.FailureListener;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

/**
 * Default implementation {@link org.apache.flink.core.failurelistener.FailureListener} that records
 * metrics of job failures.
 */
public class DefaultFailureListener implements FailureListener {
    private final Counter failureCount;

    public DefaultFailureListener(MetricGroup metricGroup) {
        this.failureCount = metricGroup.counter(MetricNames.NUM_JOB_FAILURES);
    }

    @Override
    public void onFailure(Throwable cause, boolean globalFailure) {
        failureCount.inc();
    }
}
