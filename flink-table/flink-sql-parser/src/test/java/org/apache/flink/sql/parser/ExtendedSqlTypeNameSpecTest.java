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

import org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec;

import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatCode;

class ExtendedSqlTypeNameSpecTest {

    @Test
    void testEquals() {
        ExtendedSqlRowTypeNameSpec nameSpec =
                new ExtendedSqlRowTypeNameSpec(
                        SqlParserPos.ZERO,
                        Arrays.asList(
                                new SqlIdentifier("column1", SqlParserPos.ZERO),
                                new SqlIdentifier("column2", SqlParserPos.ZERO)),
                        Arrays.asList(
                                new SqlDataTypeSpec(
                                        new SqlBasicTypeNameSpec(
                                                SqlTypeName.INTEGER, SqlParserPos.ZERO),
                                        SqlParserPos.ZERO),
                                new SqlDataTypeSpec(
                                        new SqlBasicTypeNameSpec(
                                                SqlTypeName.INTEGER, SqlParserPos.ZERO),
                                        SqlParserPos.ZERO)),
                        Collections.emptyList(),
                        true);
        assertThatCode(() -> nameSpec.equalsDeep(nameSpec, Litmus.THROW))
                .doesNotThrowAnyException();
    }
}
