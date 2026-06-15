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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.data.StringData;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for GEOGRAPHY accessor conversion utilities. */
class GeographyAccessorFunctionsTest {

    private static final byte[] POINT_WKB =
            new byte[] {
                1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xF0, 0x3F, 0, 0, 0, 0, 0, 0, 0, 0x40
            };

    @Test
    void testAsTextReturnsWkt() {
        final GeographyData geography = GeographyData.fromBytes(POINT_WKB);

        assertThat(GeographyConversionUtils.asText(geography))
                .isEqualTo(StringData.fromString("POINT (1 2)"));
    }

    @Test
    void testAsTextSupportsEmptyGeometry() {
        final GeographyData geography =
                GeographyConversionUtils.fromWkt(StringData.fromString("POINT EMPTY"));

        assertThat(GeographyConversionUtils.asText(geography))
                .isEqualTo(StringData.fromString("POINT EMPTY"));
    }

    @Test
    void testAsWkbReturnsRawIsoWkb() {
        final GeographyData geography = GeographyData.fromBytes(POINT_WKB);

        assertThat(GeographyConversionUtils.asWkb(geography))
                .hasSize(POINT_WKB.length)
                .isEqualTo(POINT_WKB);
    }

    @Test
    void testAccessorsPropagateNull() {
        assertThat(GeographyConversionUtils.asText(null)).isNull();
        assertThat(GeographyConversionUtils.asWkb(null)).isNull();
    }
}
