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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.utils.EncodingUtils;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Representation of a watermark specification in a {@link ResolvedSchema}.
 *
 * <p>It defines the rowtime attribute and a {@link ResolvedExpression} for watermark generation.
 */
@PublicEvolving
public final class WatermarkSpec {

    private final String rowtimeAttribute;

    private final ResolvedExpression watermarkExpression;

    private WatermarkSpec(String rowtimeAttribute, ResolvedExpression watermarkExpression) {
        this.rowtimeAttribute =
                checkNotNull(rowtimeAttribute, "Rowtime attribute must not be null.");
        this.watermarkExpression =
                checkNotNull(watermarkExpression, "Watermark expression must not be null.");
    }

    public static WatermarkSpec of(
            String rowtimeAttribute, ResolvedExpression watermarkExpression) {
        return new WatermarkSpec(rowtimeAttribute, watermarkExpression);
    }

    /**
     * Returns the name of a rowtime attribute.
     *
     * <p>The referenced attribute must be present in the {@link ResolvedSchema} and must be of
     * {@link TimestampType}.
     */
    public String getRowtimeAttribute() {
        return rowtimeAttribute;
    }

    /** Returns the {@link ResolvedExpression} for watermark generation. */
    public ResolvedExpression getWatermarkExpression() {
        return watermarkExpression;
    }

    public String asSummaryString() {
        return "WATERMARK FOR "
                + String.join(".", EncodingUtils.escapeIdentifier(rowtimeAttribute))
                + ": "
                + watermarkExpression.getOutputDataType()
                + " AS "
                + watermarkExpression.asSummaryString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final WatermarkSpec that = (WatermarkSpec) o;
        return rowtimeAttribute.equals(that.rowtimeAttribute)
                && watermarkExpression.equals(that.watermarkExpression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowtimeAttribute, watermarkExpression);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
