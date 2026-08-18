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

package org.apache.flink.table.data;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.data.binary.BinaryGeographyData;
import org.apache.flink.table.types.logical.GeographyType;

/**
 * An internal data structure representing data of {@link GeographyType}.
 *
 * <p>The v1 runtime contract uses OGC:CRS84 and supports the seven standard 2D ISO WKB subtype
 * codes defined by the OGC Simple Feature Access specification. Extended WKB variants such as Z,
 * M, ZM, and EWKB are outside this contract. CRS and SRID metadata are not encoded in the ISO WKB
 * payload and must be handled at the construction or connector boundary.
 *
 * <p>References:
 *
 * <ul>
 *   <li><a href="https://www.ogc.org/standards/sfa">OGC Simple Feature Access</a>
 *   <li><a href="https://www.opengis.net/def/crs/OGC/1.3/CRS84">OGC:CRS84 definition</a>
 *   <li><a href="https://postgis.net/docs/using_postgis_dbmanagement.html#EWKB_EWKT">PostGIS
 *       EWKB and EWKT documentation</a>
 * </ul>
 */
@PublicEvolving
public interface GeographyData {

    /** Supported 2D ISO WKB subtype ID for Point geometries. */
    int POINT = 1;

    /** Supported 2D ISO WKB subtype ID for LineString geometries. */
    int LINE_STRING = 2;

    /** Supported 2D ISO WKB subtype ID for Polygon geometries. */
    int POLYGON = 3;

    /** Supported 2D ISO WKB subtype ID for MultiPoint geometries. */
    int MULTI_POINT = 4;

    /** Supported 2D ISO WKB subtype ID for MultiLineString geometries. */
    int MULTI_LINE_STRING = 5;

    /** Supported 2D ISO WKB subtype ID for MultiPolygon geometries. */
    int MULTI_POLYGON = 6;

    /** Supported 2D ISO WKB subtype ID for GeometryCollection geometries. */
    int GEOMETRY_COLLECTION = 7;

    /**
     * Converts this {@link GeographyData} object to an ISO WKB byte array.
     *
     * <p>Note: The returned byte array may be reused.
     */
    byte[] toBytes();

    /** Returns the supported 2D ISO WKB subtype ID. */
    int subtypeId();

    /** Returns the size in bytes of the ISO WKB payload. */
    int sizeInBytes();

    // ------------------------------------------------------------------------------------------
    // Construction Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Creates an instance of {@link GeographyData} from the given ISO WKB byte array. Returns
     * {@code null} if the input is {@code null}.
     */
    static GeographyData fromBytes(byte[] bytes) {
        return BinaryGeographyData.fromBytes(bytes);
    }

    /**
     * Creates an instance of {@link GeographyData} from the given ISO WKB byte range. Returns
     * {@code null} if the input is {@code null}.
     */
    static GeographyData fromBytes(byte[] bytes, int offset, int numBytes) {
        return BinaryGeographyData.fromBytes(bytes, offset, numBytes);
    }
}
