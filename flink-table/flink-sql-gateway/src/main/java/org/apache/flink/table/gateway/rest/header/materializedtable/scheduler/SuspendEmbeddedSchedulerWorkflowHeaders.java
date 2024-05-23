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

package org.apache.flink.table.gateway.rest.header.materializedtable.scheduler;

import org.apache.flink.runtime.rest.HttpMethodWrapper;

/** Message headers for suspend workflow in embedded scheduler. */
public class SuspendEmbeddedSchedulerWorkflowHeaders
        extends AbstractEmbeddedSchedulerWorkflowHeaders {

    private static final SuspendEmbeddedSchedulerWorkflowHeaders INSTANCE =
            new SuspendEmbeddedSchedulerWorkflowHeaders();

    public static final String URL = "/workflow/embedded-scheduler/suspend";

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public String getDescription() {
        return "Suspend workflow";
    }

    public static SuspendEmbeddedSchedulerWorkflowHeaders getInstance() {
        return INSTANCE;
    }
}
