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

package org.apache.flink.table.api;

import org.apache.flink.api.common.errors.FlinkErrorTestUtils;
import org.apache.flink.table.catalog.ObjectIdentifier;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableError}. */
class TableErrorTest {

    @Test
    void testErrors() {
        FlinkErrorTestUtils.assertErrors(TableError.class, TableErrorTest::sampleArgument);
    }

    @Test
    void testObjectNameIsSqlIdentifier() {
        final ObjectIdentifier identifier = ObjectIdentifier.of("c", "my.db", "t`1");

        final ValidationException e =
                new ValidationException(TableError.TABLE_NOT_FOUND, identifier);

        assertThat(e)
                .hasMessage(
                        "Table `c`.`my.db`.`t``1` does not exist. "
                                + "Check the name and the current catalog and database. "
                                + "SQLSTATE: 42P01");
        assertThat(e.getRedactedMessage())
                .isEqualTo(
                        "Table <redacted> does not exist. "
                                + "Check the name and the current catalog and database. "
                                + "SQLSTATE: 42P01");
    }

    private static Object sampleArgument(Class<?> type) {
        if (type == ObjectIdentifier.class) {
            return ObjectIdentifier.of("c", "db", "t");
        }
        throw new IllegalArgumentException("No sample argument for " + type.getName());
    }
}
