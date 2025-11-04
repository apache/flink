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

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

/**
 * Extends Calcite's {@link org.apache.calcite.sql.validate.SqlValidator}. Right now it extends by
 * row struct kind depending on a table config option value {@code
 * table.legacy-extended-row-struct-kind-as-fully-qualified}. However, in future more config option
 * dependent fields might be added here.
 */
public class FlinkSqlParsingValidator extends SqlValidatorImpl {
    private final StructKind extendedRowStructKind;

    protected FlinkSqlParsingValidator(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            Config config,
            StructKind extendedRowStructKind) {
        super(opTab, catalogReader, typeFactory, config);
        this.extendedRowStructKind = extendedRowStructKind;
    }

    public final StructKind getExtendedRowStructKind() {
        return extendedRowStructKind;
    }
}
