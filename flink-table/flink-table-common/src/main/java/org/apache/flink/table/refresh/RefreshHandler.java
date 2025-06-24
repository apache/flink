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

package org.apache.flink.table.refresh;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.Catalog;

/**
 * This interface represents the meta information of current materialized table background refresh
 * pipeline. The refresh mode maybe continuous or full, the meta information in the two modes is not
 * consistent, so user need to implementation this interface according to different case.
 *
 * <p>In continuous mode, the meta information maybe contains { "clusterType": "yarn", "clusterId":
 * "xxx", "jobId": "yyyy" }.
 *
 * <p>In full mode, the meta information maybe contains { "endpoint": "xxx", "workflowId": "yyy" }.
 * Due to user may use different workflow scheduler in this mode, user should implement this
 * interface according to their plugin.
 *
 * <p>This interface will be serialized to bytes by {@link RefreshHandlerSerializer}, then store to
 * {@link Catalog} for further operation.
 */
@PublicEvolving
public interface RefreshHandler {

    /**
     * Returns a string that summarizes this refresh handler meta information for printing to a
     * console or log.
     */
    String asSummaryString();
}
