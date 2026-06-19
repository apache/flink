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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.FunctionParam;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/** Spec to describe {@code ML_PREDICT}. */
public class MLPredictSpec {

    public static final String FIELD_NAME_FEATURES = "features";
    public static final String FIELD_NAME_RUNTIME_CONFIG = "runtimeConfig";

    @JsonProperty(FIELD_NAME_FEATURES)
    private final List<FunctionParam> features;

    @JsonProperty(FIELD_NAME_RUNTIME_CONFIG)
    private final Map<String, String> runtimeConfig;

    @JsonCreator
    public MLPredictSpec(
            @JsonProperty(FIELD_NAME_FEATURES) List<FunctionParam> features,
            @JsonProperty(FIELD_NAME_RUNTIME_CONFIG) Map<String, String> runtimeConfig) {
        this.features = features;
        this.runtimeConfig = runtimeConfig;
    }

    public List<FunctionParam> getFeatures() {
        return features;
    }

    public Map<String, String> getRuntimeConfig() {
        return runtimeConfig;
    }
}
