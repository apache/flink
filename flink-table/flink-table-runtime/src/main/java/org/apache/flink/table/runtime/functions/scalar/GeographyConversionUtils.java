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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.data.StringData;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

import java.util.regex.Pattern;

/** Utilities for converting GEOGRAPHY values to and from portable OGC encodings. */
@Internal
final class GeographyConversionUtils {

    private static final Pattern WKT_DIMENSION_TOKEN =
            Pattern.compile(
                    "\\b(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\\s+(ZM|Z|M)\\b",
                    Pattern.CASE_INSENSITIVE);
    private static final WKTReader WKT_READER = createWktReader();
    private static final WKBReader WKB_READER = new WKBReader();
    private static final WKBWriter WKB_WRITER =
            new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN, false);
    private static final WKTWriter WKT_WRITER = new WKTWriter(2);

    private GeographyConversionUtils() {}

    static GeographyData fromWkt(StringData text) {
        if (text == null) {
            return null;
        }

        final String wkt = text.toString();
        if (WKT_DIMENSION_TOKEN.matcher(wkt).find()) {
            throw new TableRuntimeException(
                    "Invalid GEOGRAPHY WKT. Only 2D coordinates are supported.");
        }

        final Geometry geometry;
        try {
            geometry = WKT_READER.read(wkt);
        } catch (ParseException | IllegalArgumentException e) {
            throw new TableRuntimeException("Invalid GEOGRAPHY WKT.", e);
        }
        validateGeometry(geometry);
        return fromValidatedGeometry(geometry);
    }

    static GeographyData fromWkb(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        final GeographyData geography;
        try {
            geography = GeographyData.fromBytes(bytes);
        } catch (TableRuntimeException e) {
            throw new TableRuntimeException("Invalid GEOGRAPHY WKB.", e);
        }

        validateGeometry(readGeometry(geography));
        return geography;
    }

    static StringData asText(GeographyData geography) {
        if (geography == null) {
            return null;
        }

        return StringData.fromString(WKT_WRITER.write(readGeometry(geography)));
    }

    static byte[] asWkb(GeographyData geography) {
        if (geography == null) {
            return null;
        }

        return geography.toBytes();
    }

    private static GeographyData fromValidatedGeometry(Geometry geometry) {
        try {
            return GeographyData.fromBytes(WKB_WRITER.write(geometry));
        } catch (TableRuntimeException e) {
            throw new TableRuntimeException("Invalid GEOGRAPHY WKT.", e);
        }
    }

    private static Geometry readGeometry(GeographyData geography) {
        try {
            return WKB_READER.read(geography.toBytes());
        } catch (ParseException e) {
            throw new TableRuntimeException("Invalid GEOGRAPHY WKB.", e);
        }
    }

    private static WKTReader createWktReader() {
        final WKTReader reader = new WKTReader();
        reader.setIsOldJtsCoordinateSyntaxAllowed(false);
        return reader;
    }

    private static void validateGeometry(Geometry geometry) {
        validateSubtype(geometry);
        if (!geometry.isEmpty()) {
            geometry.apply(new Crs84CoordinateValidator());
        }
    }

    private static void validateSubtype(Geometry geometry) {
        switch (geometry.getGeometryType()) {
            case Geometry.TYPENAME_POINT:
            case Geometry.TYPENAME_LINESTRING:
            case Geometry.TYPENAME_POLYGON:
            case Geometry.TYPENAME_MULTIPOINT:
            case Geometry.TYPENAME_MULTILINESTRING:
            case Geometry.TYPENAME_MULTIPOLYGON:
            case Geometry.TYPENAME_GEOMETRYCOLLECTION:
                return;
            default:
                throw new TableRuntimeException(
                        "Unsupported GEOGRAPHY subtype: " + geometry.getGeometryType() + ".");
        }
    }

    private static final class Crs84CoordinateValidator implements CoordinateSequenceFilter {

        @Override
        public void filter(CoordinateSequence sequence, int i) {
            if (sequence.hasZ() || sequence.hasM()) {
                throw new TableRuntimeException(
                        "Invalid GEOGRAPHY coordinates. Only 2D coordinates are supported.");
            }

            final double longitude = sequence.getX(i);
            final double latitude = sequence.getY(i);
            if (longitude < -180D || longitude > 180D) {
                throw new TableRuntimeException(
                        String.format(
                                "Invalid GEOGRAPHY longitude %.12f. Expected range is [-180, 180].",
                                longitude));
            }
            if (latitude < -90D || latitude > 90D) {
                throw new TableRuntimeException(
                        String.format(
                                "Invalid GEOGRAPHY latitude %.12f. Expected range is [-90, 90].",
                                latitude));
            }
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public boolean isGeometryChanged() {
            return false;
        }
    }
}
