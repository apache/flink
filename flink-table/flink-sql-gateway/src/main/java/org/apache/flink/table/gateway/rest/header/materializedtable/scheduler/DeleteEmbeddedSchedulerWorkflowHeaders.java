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

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.runtime.rest.HttpMethodWrapper;

/** Message headers for delete workflow in embedded scheduler. */
@Documentation.ExcludeFromDocumentation("The embedded rest api.")
public class DeleteEmbeddedSchedulerWorkflowHeaders
        extends AbstractEmbeddedSchedulerWorkflowHeaders {

    private static final DeleteEmbeddedSchedulerWorkflowHeaders INSTANCE =
            new DeleteEmbeddedSchedulerWorkflowHeaders();

    public static final String URL = "/workflow/embedded-scheduler/delete";

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.DELETE;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public String getDescription() {
        return "Delete workflow";
    }

    public static DeleteEmbeddedSchedulerWorkflowHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String operationId() {
        return "deleteWorkflow";
    }
}
