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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.types.logical.GeographyType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for generated projections involving {@link GeographyData}. */
class ProjectionCodeGeneratorGeographyTest {

    private static final byte[] POINT_WKB =
            new byte[] {
                1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xF0, 0x3F, 0, 0, 0, 0, 0, 0, 0, 0x40
            };

    @Test
    void testGeneratedProjectionForGeography() {
        RowType inputType = RowType.of(new IntType(), new GeographyType(), new GeographyType());
        RowType outputType = RowType.of(new GeographyType(), new GeographyType(), new IntType());
        GenericRowData input = GenericRowData.of(7, GeographyData.fromBytes(POINT_WKB), null);

        Projection projection =
                ProjectionCodeGenerator.generateProjection(
                                new CodeGeneratorContext(
                                        new Configuration(),
                                        Thread.currentThread().getContextClassLoader()),
                                "GeographyProjection",
                                inputType,
                                outputType,
                                new int[] {1, 2, 0})
                        .newInstance(Thread.currentThread().getContextClassLoader());

        BinaryRowData output = (BinaryRowData) projection.apply(input);

        assertThat(output.getGeography(0).toBytes()).isEqualTo(POINT_WKB);
        assertThat(output.isNullAt(1)).isTrue();
        assertThat(output.getInt(2)).isEqualTo(7);
    }
}
