/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.sql.parser.SqlParserFixture;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * Verifies that Flink's extended SQL parser correctly handles all standard SQL syntax defined in
 * Calcite's {@link SqlParserTest}. Methods overridden with empty bodies represent Calcite syntax
 * that Flink intentionally does not support.
 */
@Execution(CONCURRENT)
class FlinkSqlParserCalciteTest extends SqlParserTest {

    @Override
    public SqlParserFixture fixture() {
        return super.fixture().withConfig(c -> c.withParserFactory(FlinkSqlParserImpl.FACTORY));
    }

    // -- Calcite syntax not supported by Flink's parser --

    @Test
    void testArrayFunction() {}

    @Test
    void testArrayQueryConstructor() {}

    @Test
    void testPercentileCont() {}

    @Test
    void testPercentileContBigQuery() {}

    @Test
    void testPercentileDisc() {}

    @Test
    void testPercentileDiscBigQuery() {}

    @Test
    void testMapQueryConstructor() {}

    @Test
    void testMultisetQueryConstructor() {}

    @Test
    void testMeasure() {}

    @Disabled
    @Test
    void testDescribeSchema() {}

    @Disabled
    @Test
    void testDescribeStatement() {}

    @Disabled
    @Test
    void testGroupConcat() {}

    @Disabled
    @Test
    void testExplainAsDot() {}

    @Disabled
    @Test
    void testStringAgg() {}

    // -- Calcite syntax overridden by Flink (tested in split test classes) --

    @Test
    void testArrayAgg() {}

    @Test
    void testCastAsRowType() {}

    @Test
    void testDescribeTable() {}

    @Test
    void testExplain() {}

    @Test
    void testExplainJsonFormat() {}

    @Test
    void testExplainWithImpl() {}

    @Test
    void testExplainWithoutImpl() {}

    @Test
    void testExplainWithType() {}

    @Test
    void testExplainAsXml() {}

    @Test
    void testExplainAsJson() {}

    @Test
    void testExplainInsert() {}

    @Test
    void testExplainUpsert() {}

    @Test
    void testSqlOptions() {}
}
