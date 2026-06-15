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

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.data.StringData;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for GEOGRAPHY constructor conversion utilities. */
class GeographyConstructorFunctionsTest {

    private static final byte[] POINT_WKB =
            new byte[] {
                1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xF0, 0x3F, 0, 0, 0, 0, 0, 0, 0, 0x40
            };

    @Test
    void testFromWktCreatesIsoWkb() {
        final GeographyData geography =
                GeographyConversionUtils.fromWkt(StringData.fromString("POINT (1 2)"));

        assertThat(geography.subtypeId()).isEqualTo(GeographyData.POINT);
        assertThat(geography.toBytes()).isEqualTo(POINT_WKB);
    }

    @Test
    void testFromWktAllowsEmptyGeometry() {
        final GeographyData geography =
                GeographyConversionUtils.fromWkt(StringData.fromString("POINT EMPTY"));

        assertThat(geography.subtypeId()).isEqualTo(GeographyData.POINT);
    }

    @Test
    void testFromWkbKeepsRawIsoWkb() {
        final GeographyData geography = GeographyConversionUtils.fromWkb(POINT_WKB);

        assertThat(geography.toBytes()).isEqualTo(POINT_WKB);
    }

    @Test
    void testConstructorsPropagateNull() {
        assertThat(GeographyConversionUtils.fromWkt(null)).isNull();
        assertThat(GeographyConversionUtils.fromWkb(null)).isNull();
    }

    @Test
    void testFromWktRejectsMalformedInput() {
        assertThatThrownBy(() -> GeographyConversionUtils.fromWkt(StringData.fromString("not wkt")))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Invalid GEOGRAPHY WKT.");
    }

    @Test
    void testFromWktRejectsThreeDimensionalInput() {
        assertThatThrownBy(
                        () ->
                                GeographyConversionUtils.fromWkt(
                                        StringData.fromString("POINT Z (1 2 3)")))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Only 2D coordinates are supported");
    }

    @Test
    void testFromWktRejectsCoordinatesOutsideCrs84Range() {
        assertThatThrownBy(
                        () ->
                                GeographyConversionUtils.fromWkt(
                                        StringData.fromString("POINT (181 2)")))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Expected range is [-180, 180]");

        assertThatThrownBy(
                        () ->
                                GeographyConversionUtils.fromWkt(
                                        StringData.fromString("POINT (1 91)")))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Expected range is [-90, 90]");
    }

    @Test
    void testFromWkbRejectsMalformedInput() {
        assertThatThrownBy(() -> GeographyConversionUtils.fromWkb(new byte[] {1, 1, 0, 0}))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Invalid GEOGRAPHY WKB.");
    }
}
