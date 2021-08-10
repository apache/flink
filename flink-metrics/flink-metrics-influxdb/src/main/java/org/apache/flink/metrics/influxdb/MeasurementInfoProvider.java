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

package org.apache.flink.metrics.influxdb;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

class MeasurementInfoProvider implements MetricInfoProvider<MeasurementInfo> {
    @VisibleForTesting static final char SCOPE_SEPARATOR = '_';
    private static final String POINT_DELIMITER = "\n";

    private static final CharacterFilter CHARACTER_FILTER =
            new CharacterFilter() {
                private final Pattern notAllowedCharacters = Pattern.compile("[^a-zA-Z0-9:_]");

                @Override
                public String filterCharacters(String input) {
                    return notAllowedCharacters.matcher(input).replaceAll("_");
                }
            };

    public MeasurementInfoProvider() {}

    @Override
    public MeasurementInfo getMetricInfo(String metricName, MetricGroup group) {
        return new MeasurementInfo(getScopedName(metricName, group), getTags(group));
    }

    private static Map<String, String> getTags(MetricGroup group) {
        // Keys are surrounded by brackets: remove them, transforming "<name>" to "name".
        Map<String, String> tags = new HashMap<>();
        for (Map.Entry<String, String> variable : group.getAllVariables().entrySet()) {
            String name = variable.getKey();
            tags.put(
                    normalize(name.substring(1, name.length() - 1)),
                    normalize(variable.getValue()));
        }
        return tags;
    }

    private static String getScopedName(String metricName, MetricGroup group) {
        return getLogicalScope(group) + SCOPE_SEPARATOR + metricName;
    }

    private static String getLogicalScope(MetricGroup group) {
        return LogicalScopeProvider.castFrom(group)
                .getLogicalScope(CHARACTER_FILTER, SCOPE_SEPARATOR);
    }

    private static String normalize(String value) {
        return value.replace(POINT_DELIMITER, "");
    }
}
