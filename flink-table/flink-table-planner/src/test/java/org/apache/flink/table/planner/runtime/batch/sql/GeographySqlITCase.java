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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.callSql;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end SQL coverage for GEOGRAPHY values. */
class GeographySqlITCase extends BatchTestBase {

    private static final byte[] LINESTRING_WKB =
            new byte[] {
                1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, -16, 63
            };

    @Test
    void testGeographySqlInsertAndRoundTripAccessors() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(
                        Row.of(1L, "POINT (1 2)"),
                        Row.of(2L, "LINESTRING (0 0, 1 1)"),
                        Row.of(3L, null));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE GeographySource(id BIGINT, wkt STRING) WITH "
                                + "('connector' = 'COLLECTION')")
                .await();

        final List<Row> geographies =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql(
                                        "SELECT id, ST_GEOGFROMTEXT(wkt) AS geog "
                                                + "FROM GeographySource ORDER BY id")
                                .collect());

        assertThat(geographies).hasSize(3);
        assertThat(geographies.get(0).getField(0)).isEqualTo(1L);
        assertThat(((GeographyData) geographies.get(0).getField(1)).subtypeId())
                .isEqualTo(GeographyData.POINT);
        assertThat(geographies.get(1).getField(0)).isEqualTo(2L);
        assertThat(((GeographyData) geographies.get(1).getField(1)).toBytes())
                .isEqualTo(LINESTRING_WKB);
        assertThat(geographies.get(2).getField(0)).isEqualTo(3L);
        assertThat(geographies.get(2).getField(1)).isNull();

        final List<Row> selectedRows =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql(
                                        "SELECT id, "
                                                + "ST_ASTEXT(geog), "
                                                + "ST_ASWKB(geog), "
                                                + "ST_ASTEXT(ST_GEOGFROMWKB(ST_ASWKB(geog))) "
                                                + "FROM ("
                                                + "SELECT id, ST_GEOGFROMTEXT(wkt) AS geog "
                                                + "FROM GeographySource) "
                                                + "WHERE geog IS NOT NULL "
                                                + "ORDER BY id")
                                .collect());

        assertThat(selectedRows).hasSize(2);
        assertThat(selectedRows.get(0).getField(0)).isEqualTo(1L);
        assertThat(selectedRows.get(0).getField(1)).isEqualTo("POINT (1 2)");
        assertThat(((byte[]) selectedRows.get(0).getField(2)).length).isEqualTo(21);
        assertThat(selectedRows.get(0).getField(3)).isEqualTo("POINT (1 2)");
        assertThat(selectedRows.get(1).getField(0)).isEqualTo(2L);
        assertThat(selectedRows.get(1).getField(1)).isEqualTo("LINESTRING (0 0, 1 1)");
        assertThat((byte[]) selectedRows.get(1).getField(2)).isEqualTo(LINESTRING_WKB);
        assertThat(selectedRows.get(1).getField(3)).isEqualTo("LINESTRING (0 0, 1 1)");
    }

    @Test
    void testSpatialPredicatesAndMeasurementsRemainUnavailableInV1() {
        final String distanceSql =
                "SELECT ST_DISTANCE(ST_GEOGFROMTEXT('POINT (0 0)'), "
                        + "ST_GEOGFROMTEXT('POINT (1 1)'))";
        final String intersectsSql =
                "SELECT ST_INTERSECTS(ST_GEOGFROMTEXT('POINT (0 0)'), "
                        + "ST_GEOGFROMTEXT('POINT (1 1)'))";

        assertThatThrownBy(() -> tEnv().executeSql(distanceSql))
                .hasMessageContaining("ST_DISTANCE");
        assertThatThrownBy(() -> tEnv().executeSql(intersectsSql))
                .hasMessageContaining("ST_INTERSECTS");
    }

    @Test
    void testMixedGeographySchemaAcrossSqlAndTableRuntime() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(
                        Row.of(
                                1L,
                                "alpha",
                                true,
                                LocalDateTime.parse("2024-01-02T03:04:05.123"),
                                null,
                                "POINT (1 2)",
                                null),
                        Row.of(
                                2L,
                                "beta",
                                false,
                                LocalDateTime.parse("2024-02-03T04:05:06.789"),
                                "second row",
                                null,
                                LINESTRING_WKB),
                        Row.of(3L, null, null, null, "missing geography", null, null));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE MixedGeographySource("
                                + "id BIGINT, "
                                + "name STRING, "
                                + "active BOOLEAN, "
                                + "event_ts TIMESTAMP(3), "
                                + "note STRING, "
                                + "wkt STRING, "
                                + "wkb BYTES"
                                + ") WITH ('connector' = 'COLLECTION')")
                .await();
        tEnv().createTemporaryView(
                        "MixedGeographyInput",
                        tEnv().sqlQuery(
                                        "SELECT id, name, active, event_ts, note, "
                                                + "CASE "
                                                + "WHEN wkt IS NOT NULL THEN ST_GEOGFROMTEXT(wkt) "
                                                + "WHEN wkb IS NOT NULL THEN ST_GEOGFROMWKB(wkb) "
                                                + "ELSE CAST(NULL AS GEOGRAPHY) "
                                                + "END AS geog "
                                                + "FROM MixedGeographySource"));

        final List<Row> sinkRows =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql(
                                        "SELECT id, name, active, event_ts, note, geog "
                                                + "FROM MixedGeographyInput ORDER BY id")
                                .collect());

        assertThat(sinkRows).hasSize(3);

        assertThat(sinkRows.get(0).getField(0)).isEqualTo(1L);
        assertThat(sinkRows.get(0).getField(1)).isEqualTo("alpha");
        assertThat(sinkRows.get(0).getField(2)).isEqualTo(true);
        assertThat(String.valueOf(sinkRows.get(0).getField(3)))
                .isEqualTo("2024-01-02T03:04:05.123");
        assertThat(sinkRows.get(0).getField(4)).isNull();
        assertThat(((GeographyData) sinkRows.get(0).getField(5)).subtypeId())
                .isEqualTo(GeographyData.POINT);

        assertThat(sinkRows.get(1).getField(0)).isEqualTo(2L);
        assertThat(sinkRows.get(1).getField(1)).isEqualTo("beta");
        assertThat(sinkRows.get(1).getField(2)).isEqualTo(false);
        assertThat(String.valueOf(sinkRows.get(1).getField(3)))
                .isEqualTo("2024-02-03T04:05:06.789");
        assertThat(sinkRows.get(1).getField(4)).isEqualTo("second row");
        assertThat(((GeographyData) sinkRows.get(1).getField(5)).toBytes())
                .isEqualTo(LINESTRING_WKB);

        assertThat(sinkRows.get(2).getField(0)).isEqualTo(3L);
        assertThat(sinkRows.get(2).getField(1)).isNull();
        assertThat(sinkRows.get(2).getField(2)).isNull();
        assertThat(sinkRows.get(2).getField(3)).isNull();
        assertThat(sinkRows.get(2).getField(4)).isEqualTo("missing geography");
        assertThat(sinkRows.get(2).getField(5)).isNull();

        final Table projectedTable =
                tEnv().from("MixedGeographyInput")
                        .select(
                                $("id"),
                                $("name"),
                                $("active"),
                                $("event_ts"),
                                $("note"),
                                callSql("ST_ASTEXT(geog)"),
                                callSql("ST_ASWKB(geog)"));

        final List<Row> projectedRows =
                CollectionUtil.iteratorToList(projectedTable.execute().collect());
        projectedRows.sort(Comparator.comparing(row -> (Long) row.getField(0)));

        assertThat(projectedRows).hasSize(3);
        assertThat(projectedRows.get(0).getField(0)).isEqualTo(1L);
        assertThat(projectedRows.get(0).getField(1)).isEqualTo("alpha");
        assertThat(projectedRows.get(0).getField(2)).isEqualTo(true);
        assertThat(String.valueOf(projectedRows.get(0).getField(3)))
                .isEqualTo("2024-01-02T03:04:05.123");
        assertThat(projectedRows.get(0).getField(4)).isNull();
        assertThat(projectedRows.get(0).getField(5)).isEqualTo("POINT (1 2)");
        assertThat(((byte[]) projectedRows.get(0).getField(6)).length).isEqualTo(21);

        assertThat(projectedRows.get(1).getField(0)).isEqualTo(2L);
        assertThat(projectedRows.get(1).getField(1)).isEqualTo("beta");
        assertThat(projectedRows.get(1).getField(2)).isEqualTo(false);
        assertThat(String.valueOf(projectedRows.get(1).getField(3)))
                .isEqualTo("2024-02-03T04:05:06.789");
        assertThat(projectedRows.get(1).getField(4)).isEqualTo("second row");
        assertThat(projectedRows.get(1).getField(5)).isEqualTo("LINESTRING (0 0, 1 1)");
        assertThat((byte[]) projectedRows.get(1).getField(6)).isEqualTo(LINESTRING_WKB);

        assertThat(projectedRows.get(2).getField(0)).isEqualTo(3L);
        assertThat(projectedRows.get(2).getField(1)).isNull();
        assertThat(projectedRows.get(2).getField(2)).isNull();
        assertThat(projectedRows.get(2).getField(3)).isNull();
        assertThat(projectedRows.get(2).getField(4)).isEqualTo("missing geography");
        assertThat(projectedRows.get(2).getField(5)).isNull();
        assertThat(projectedRows.get(2).getField(6)).isNull();
    }
}
