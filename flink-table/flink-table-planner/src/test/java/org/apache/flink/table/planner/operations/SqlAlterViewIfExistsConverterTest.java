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

package org.apache.flink.table.planner.operations;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.operations.NopOperation;
import org.apache.flink.table.operations.Operation;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@code ALTER VIEW ... IF EXISTS} in {@link SqlNodeToOperationConversion}. */
class SqlAlterViewIfExistsConverterTest extends SqlNodeToOperationConversionTestBase {

    @Test
    void testAlterViewIfExistsOnMissingView() {
        // with the flag, a missing view turns the statement into a no-op
        final Operation rename = parse("ALTER VIEW IF EXISTS missing_view RENAME TO new_name");
        assertThat(rename).isInstanceOf(NopOperation.class);

        final Operation alterAs = parse("ALTER VIEW IF EXISTS missing_view AS SELECT 1");
        assertThat(alterAs).isInstanceOf(NopOperation.class);

        // without the flag, a missing view is still an error
        assertThatThrownBy(() -> parse("ALTER VIEW missing_view RENAME TO new_name"))
                .isInstanceOf(ValidationException.class);
        assertThatThrownBy(() -> parse("ALTER VIEW missing_view AS SELECT 1"))
                .isInstanceOf(ValidationException.class);
    }
}
