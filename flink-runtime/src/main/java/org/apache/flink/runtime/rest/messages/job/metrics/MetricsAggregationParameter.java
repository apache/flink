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

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.util.StringUtils;

import java.util.Locale;

/** TODO: add javadoc. */
public class MetricsAggregationParameter
        extends MessageQueryParameter<MetricsAggregationParameter.AggregationMode> {

    protected MetricsAggregationParameter() {
        super("agg", MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public AggregationMode convertStringToValue(String value) throws ConversionException {
        try {
            return AggregationMode.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException iae) {
            throw new ConversionException("Not a valid aggregation: " + value, iae);
        }
    }

    @Override
    public String convertValueToString(AggregationMode value) {
        return value.name().toLowerCase();
    }

    @Override
    public String getDescription() {
        return "Comma-separated list of aggregation modes which should be calculated. "
                + "Available aggregations are: "
                + StringUtils.toQuotedListString(AggregationMode.values())
                + '.';
    }

    /** The available aggregations. */
    public enum AggregationMode {
        MIN,
        MAX,
        SUM,
        AVG
    }
}
