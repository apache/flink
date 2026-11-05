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

package org.apache.flink.table.delegation.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.refresh.RefreshHandler;
import org.apache.flink.table.workflow.WorkflowScheduler;

import java.util.Map;

/**
 * The capability to schedule periodic refresh workflows for full-mode materialized tables, exposed
 * by a {@link MaterializedTableJobSubmitter} only when the deployment can provide it.
 */
@Internal
public interface RefreshWorkflowContext {

    /** The scheduler that creates, modifies and deletes periodic refresh workflows. */
    WorkflowScheduler<? extends RefreshHandler> scheduler();

    /** The REST endpoint URL a scheduled workflow uses to reach the cluster managing refreshes. */
    String restEndpointUrl();

    /** The session configuration a scheduled refresh job is initialized with. */
    Map<String, String> sessionInitializationConf();
}
