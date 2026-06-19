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
import org.apache.flink.table.catalog.ObjectIdentifier;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * {@link CreateRefreshWorkflow} provides the related information to create periodic refresh
 * workflow of {@link CatalogMaterializedTable}.
 */
@PublicEvolving
public class CreatePeriodicRefreshWorkflow implements CreateRefreshWorkflow {

    private final ObjectIdentifier materializedTableIdentifier;
    private final String descriptionStatement;
    private final String cronExpression;
    private final @Nullable Map<String, String> initConfig;
    private final @Nullable Map<String, String> executionConfig;

    // The SQL Gateway rest endpoint url that used for execute refresh operation
    private final String restEndpointUrl;

    public CreatePeriodicRefreshWorkflow(
            ObjectIdentifier materializedTableIdentifier,
            String descriptionStatement,
            String cronExpression,
            Map<String, String> initConfig,
            Map<String, String> executionConfig,
            String restEndpointUrl) {
        this.materializedTableIdentifier = materializedTableIdentifier;
        this.descriptionStatement = descriptionStatement;
        this.cronExpression = cronExpression;
        this.initConfig = initConfig;
        this.executionConfig = executionConfig;
        this.restEndpointUrl = restEndpointUrl;
    }

    public ObjectIdentifier getMaterializedTableIdentifier() {
        return materializedTableIdentifier;
    }

    public String getDescriptionStatement() {
        return descriptionStatement;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    @Nullable
    public Map<String, String> getInitConfig() {
        return initConfig;
    }

    @Nullable
    public Map<String, String> getExecutionConfig() {
        return executionConfig;
    }

    public String getRestEndpointUrl() {
        return restEndpointUrl;
    }
}
