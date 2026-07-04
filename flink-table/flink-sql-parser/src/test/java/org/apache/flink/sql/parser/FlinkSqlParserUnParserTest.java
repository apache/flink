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

import org.apache.calcite.sql.parser.SqlParserFixture;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * Unparser (round-trip) tests for all Flink SQL parser syntax categories.
 *
 * <p>Each {@link Nested} inner class extends the corresponding parser test class and overrides
 * {@link #fixture()} to use {@link SqlParserTest.UnparsingTesterImpl}, which verifies that parse
 * &rarr; unparse &rarr; re-parse produces consistent results.
 */
@Execution(CONCURRENT)
class FlinkSqlParserUnParserTest {

    @Nested
    class CatalogUnParserTest extends FlinkSqlParserCatalogTest {
        @Override
        public SqlParserFixture fixture() {
            return super.fixture().withTester(new DeepCopyUnparsingTesterImpl());
        }
    }

    @Nested
    class MetadataUnParserTest extends FlinkSqlParserMetadataTest {
        @Override
        public SqlParserFixture fixture() {
            return super.fixture().withTester(new DeepCopyUnparsingTesterImpl());
        }
    }

    @Nested
    class CreateTableUnParserTest extends FlinkSqlParserCreateTableTest {
        @Override
        public SqlParserFixture fixture() {
            return super.fixture().withTester(new DeepCopyUnparsingTesterImpl());
        }
    }

    @Nested
    class AlterTableUnParserTest extends FlinkSqlParserAlterTableTest {
        @Override
        public SqlParserFixture fixture() {
            return super.fixture().withTester(new DeepCopyUnparsingTesterImpl());
        }
    }

    @Nested
    class CtasUnParserTest extends FlinkSqlParserCtasTest {
        @Override
        public SqlParserFixture fixture() {
            return super.fixture().withTester(new DeepCopyUnparsingTesterImpl());
        }
    }

    @Nested
    class ViewUnParserTest extends FlinkSqlParserViewTest {
        @Override
        public SqlParserFixture fixture() {
            return super.fixture().withTester(new DeepCopyUnparsingTesterImpl());
        }
    }

    @Nested
    class ModelUnParserTest extends FlinkSqlParserModelTest {
        @Override
        public SqlParserFixture fixture() {
            return super.fixture().withTester(new DeepCopyUnparsingTesterImpl());
        }
    }

    @Nested
    class ConnectionUnParserTest extends FlinkSqlParserConnectionTest {
        @Override
        public SqlParserFixture fixture() {
            return super.fixture().withTester(new DeepCopyUnparsingTesterImpl());
        }
    }

    @Nested
    class ExecuteUnParserTest extends FlinkSqlParserExecuteTest {
        @Override
        public SqlParserFixture fixture() {
            return super.fixture().withTester(new DeepCopyUnparsingTesterImpl());
        }
    }

    @Nested
    class MiscUnParserTest extends FlinkSqlParserMiscTest {
        @Override
        public SqlParserFixture fixture() {
            return super.fixture().withTester(new DeepCopyUnparsingTesterImpl());
        }
    }
}
