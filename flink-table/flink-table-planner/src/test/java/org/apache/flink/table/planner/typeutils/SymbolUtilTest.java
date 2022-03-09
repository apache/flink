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

package org.apache.flink.table.planner.typeutils;

import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.planner.typeutils.SymbolUtil.SerializableSymbol;
import org.apache.flink.table.utils.DateTimeUtils;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.SqlJsonQueryEmptyOrErrorBehavior;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.junit.Test;

import static org.apache.flink.table.planner.typeutils.SymbolUtil.calciteToCommon;
import static org.apache.flink.table.planner.typeutils.SymbolUtil.calciteToSerializable;
import static org.apache.flink.table.planner.typeutils.SymbolUtil.commonToCalcite;
import static org.apache.flink.table.planner.typeutils.SymbolUtil.serializableToCalcite;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SymbolUtil}. */
public class SymbolUtilTest {

    @Test
    public void testCalciteToSerializable() {
        final SerializableSymbol trimString = SerializableSymbol.of("TRIM", "LEADING");
        assertThat(calciteToSerializable(SqlTrimFunction.Flag.LEADING)).isEqualTo(trimString);
        assertThat(SymbolUtil.serializableToCalcite(SqlTrimFunction.Flag.class, "LEADING"))
                .isEqualTo(SqlTrimFunction.Flag.LEADING);

        final SerializableSymbol emptyOrErrorString =
                SerializableSymbol.of("JSON_QUERY_ON_EMPTY_OR_ERROR", "EMPTY_OBJECT");
        assertThat(calciteToSerializable(SqlJsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT))
                .isEqualTo(emptyOrErrorString);
        assertThat(serializableToCalcite(emptyOrErrorString))
                .isEqualTo(SqlJsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT);
    }

    @Test
    public void testCommonToCalcite() {
        // public symbol
        assertThat(commonToCalcite(TimeIntervalUnit.QUARTER)).isEqualTo(TimeUnitRange.QUARTER);
        assertThat(calciteToCommon(TimeUnitRange.QUARTER, false))
                .isEqualTo(TimeIntervalUnit.QUARTER);

        // internal symbol
        assertThat(commonToCalcite(DateTimeUtils.TimeUnitRange.QUARTER))
                .isEqualTo(TimeUnitRange.QUARTER);
        assertThat(calciteToCommon(TimeUnitRange.QUARTER, true))
                .isEqualTo(DateTimeUtils.TimeUnitRange.QUARTER);
    }
}
