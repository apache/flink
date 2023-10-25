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

package org.apache.flink.table.test.program;

import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

/** Test step for creating a table source. */
public final class SourceTestStep extends TableTestStep {

    public final List<Row> dataBeforeRestore;
    public final List<Row> dataAfterRestore;

    SourceTestStep(
            String name,
            List<String> schemaComponents,
            List<String> partitionKeys,
            Map<String, String> options,
            List<Row> dataBeforeRestore,
            List<Row> dataAfterRestore) {
        super(name, schemaComponents, partitionKeys, options);
        this.dataBeforeRestore = dataBeforeRestore;
        this.dataAfterRestore = dataAfterRestore;
    }

    @Override
    public TestKind getKind() {
        return dataBeforeRestore.isEmpty()
                ? TestKind.SOURCE_WITHOUT_DATA
                : dataAfterRestore.isEmpty()
                        ? TestKind.SOURCE_WITH_DATA
                        : TestKind.SOURCE_WITH_RESTORE_DATA;
    }
}
