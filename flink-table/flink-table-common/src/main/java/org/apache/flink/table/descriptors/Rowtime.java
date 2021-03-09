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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.sources.tsextractors.TimestampExtractor;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;

import java.util.Map;

/** Rowtime descriptor for describing an event time attribute in the schema. */
@PublicEvolving
public class Rowtime implements Descriptor {

    private final DescriptorProperties internalProperties = new DescriptorProperties(true);

    // TODO: Put these fields into RowtimeValidator once it is also ported into table-common.
    // TODO: Because these fields have polluted this API class.
    public static final String ROWTIME = "rowtime";
    public static final String ROWTIME_TIMESTAMPS_TYPE = "rowtime.timestamps.type";
    public static final String ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD = "from-field";
    public static final String ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_SOURCE = "from-source";
    public static final String ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM = "custom";
    public static final String ROWTIME_TIMESTAMPS_FROM = "rowtime.timestamps.from";
    public static final String ROWTIME_TIMESTAMPS_CLASS = "rowtime.timestamps.class";
    public static final String ROWTIME_TIMESTAMPS_SERIALIZED = "rowtime.timestamps.serialized";

    public static final String ROWTIME_WATERMARKS_TYPE = "rowtime.watermarks.type";
    public static final String ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING =
            "periodic-ascending";
    public static final String ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED = "periodic-bounded";
    public static final String ROWTIME_WATERMARKS_TYPE_VALUE_FROM_SOURCE = "from-source";
    public static final String ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM = "custom";
    public static final String ROWTIME_WATERMARKS_CLASS = "rowtime.watermarks.class";
    public static final String ROWTIME_WATERMARKS_SERIALIZED = "rowtime.watermarks.serialized";
    public static final String ROWTIME_WATERMARKS_DELAY = "rowtime.watermarks.delay";

    /**
     * Sets a built-in timestamp extractor that converts an existing {@link Long} or {@link
     * Types#SQL_TIMESTAMP} field into the rowtime attribute.
     *
     * @param fieldName The field to convert into a rowtime attribute.
     */
    public Rowtime timestampsFromField(String fieldName) {
        internalProperties.putString(
                ROWTIME_TIMESTAMPS_TYPE, ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD);
        internalProperties.putString(ROWTIME_TIMESTAMPS_FROM, fieldName);
        return this;
    }

    /**
     * Sets a built-in timestamp extractor that converts the assigned timestamps from a DataStream
     * API record into the rowtime attribute and thus preserves the assigned timestamps from the
     * source.
     *
     * <p>Note: This extractor only works in streaming environments.
     */
    public Rowtime timestampsFromSource() {
        internalProperties.putString(
                ROWTIME_TIMESTAMPS_TYPE, ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_SOURCE);
        return this;
    }

    /**
     * Sets a custom timestamp extractor to be used for the rowtime attribute.
     *
     * @param extractor The {@link TimestampExtractor} to extract the rowtime attribute from the
     *     physical type.
     */
    public Rowtime timestampsFromExtractor(TimestampExtractor extractor) {
        internalProperties.putProperties(extractor.toProperties());
        return this;
    }

    /**
     * Sets a built-in watermark strategy for ascending rowtime attributes.
     *
     * <p>Emits a watermark of the maximum observed timestamp so far minus 1. Rows that have a
     * timestamp equal to the max timestamp are not late.
     */
    public Rowtime watermarksPeriodicAscending() {
        internalProperties.putString(
                ROWTIME_WATERMARKS_TYPE, ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING);
        return this;
    }

    /**
     * Sets a built-in watermark strategy for rowtime attributes which are out-of-order by a bounded
     * time interval.
     *
     * <p>Emits watermarks which are the maximum observed timestamp minus the specified delay.
     *
     * @param delay delay in milliseconds
     */
    public Rowtime watermarksPeriodicBounded(long delay) {
        internalProperties.putString(
                ROWTIME_WATERMARKS_TYPE, ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED);
        internalProperties.putLong(ROWTIME_WATERMARKS_DELAY, delay);
        return this;
    }

    /**
     * Sets a built-in watermark strategy which indicates the watermarks should be preserved from
     * the underlying DataStream API and thus preserves the assigned watermarks from the source.
     */
    public Rowtime watermarksFromSource() {
        internalProperties.putString(
                ROWTIME_WATERMARKS_TYPE, ROWTIME_WATERMARKS_TYPE_VALUE_FROM_SOURCE);
        return this;
    }

    /** Sets a custom watermark strategy to be used for the rowtime attribute. */
    public Rowtime watermarksFromStrategy(WatermarkStrategy strategy) {
        internalProperties.putProperties(strategy.toProperties());
        return this;
    }

    /** Converts this descriptor into a set of properties. */
    @Override
    public Map<String, String> toProperties() {
        final DescriptorProperties properties = new DescriptorProperties();
        properties.putProperties(internalProperties);
        return properties.asMap();
    }
}
