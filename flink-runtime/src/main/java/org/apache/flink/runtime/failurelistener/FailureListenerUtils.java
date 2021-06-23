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

package org.apache.flink.runtime.failurelistener;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failurelistener.FailureListener;
import org.apache.flink.core.failurelistener.FailureListenerFactory;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/** Utils for creating failure listener. */
public class FailureListenerUtils {

    public static Set<FailureListener> getFailureListeners(
            Configuration configuration, JobID jobId, String jobName, MetricGroup metricGroup) {
        PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
        Iterator<FailureListenerFactory> fromPluginManager =
                pluginManager.load(FailureListenerFactory.class);

        Set<FailureListener> failureListeners = new HashSet<>();
        failureListeners.add(new DefaultFailureListener(metricGroup));
        while (fromPluginManager.hasNext()) {
            FailureListenerFactory failureListenerFactory = fromPluginManager.next();
            FailureListener failureListener =
                    failureListenerFactory.createFailureListener(
                            configuration, jobId, jobName, metricGroup);
            failureListeners.add(failureListener);
        }

        return failureListeners;
    }
}
