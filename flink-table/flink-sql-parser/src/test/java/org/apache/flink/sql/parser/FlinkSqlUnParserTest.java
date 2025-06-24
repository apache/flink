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
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * Extension to {@link FlinkSqlParserImplTest} that ensures that every expression can un-parse
 * successfully.
 */
@Execution(CONCURRENT)
class FlinkSqlUnParserTest extends FlinkSqlParserImplTest {
    // ~ Constructors -----------------------------------------------------------

    public FlinkSqlUnParserTest() {}

    // ~ Methods ----------------------------------------------------------------

    public SqlParserFixture fixture() {
        return super.fixture()
                .withTester(new UnparsingTesterImpl())
                .withConfig(c -> c.withParserFactory(FlinkSqlParserImpl.FACTORY));
    }
}
