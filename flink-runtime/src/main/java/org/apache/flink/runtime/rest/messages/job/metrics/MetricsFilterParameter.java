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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

/**
 * {@link MessageQueryParameter} for filtering metrics provided by {@link MetricStore}.
 *
 * @see org.apache.flink.runtime.rest.handler.job.metrics.AbstractMetricsHandler
 */
public class MetricsFilterParameter extends MessageQueryParameter<String> {

    private static final String QUERY_PARAMETER_NAME = "get";

    public MetricsFilterParameter() {
        super(QUERY_PARAMETER_NAME, MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public String convertStringToValue(String value) {
        return value;
    }

    @Override
    public String convertValueToString(String value) {
        return value;
    }

    @Override
    public String getDescription() {
        return "Comma-separated list of string values to select specific metrics.";
    }
}
