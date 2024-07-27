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

package org.apache.flink.table.workflow;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.refresh.RefreshHandler;

import java.util.Map;

/**
 * {@link ModifyRefreshWorkflow} provides the related information to resume refresh workflow of
 * {@link CatalogMaterializedTable}.
 */
@PublicEvolving
public class ResumeRefreshWorkflow<T extends RefreshHandler> implements ModifyRefreshWorkflow<T> {

    private final T refreshHandler;

    private final Map<String, String> dynamicOptions;

    public ResumeRefreshWorkflow(T refreshHandler, Map<String, String> dynamicOptions) {
        this.refreshHandler = refreshHandler;
        this.dynamicOptions = dynamicOptions;
    }

    public Map<String, String> getDynamicOptions() {
        return dynamicOptions;
    }

    @Override
    public T getRefreshHandler() {
        return refreshHandler;
    }
}
