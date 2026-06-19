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

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.StringReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Test class to check parser keywords. */
@Execution(CONCURRENT)
class ReservedKeywordTest {

    private static final SqlAbstractParserImpl.Metadata PARSER_METADATA =
            FlinkSqlParserImpl.FACTORY.getParser(new StringReader("")).getMetadata();

    @DisplayName("STATEMENT is a reserved keyword")
    @Test
    void testSTATEMENT() {
        assertThat(PARSER_METADATA.isKeyword("STATEMENT")).isTrue();
        assertThat(PARSER_METADATA.isNonReservedKeyword("STATEMENT")).isFalse();
    }
}
