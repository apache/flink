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

package org.apache.flink.sql.parser.type;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.calcite.ExtendedRelTypeFactory;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

/**
 * Represents the BITMAP data type, a compressed data structure for storing sets of unsigned
 * integers.
 *
 * <p>The BITMAP type does not belong to standard SQL.
 */
@Internal
public final class SqlBitmapTypeNameSpec extends SqlTypeNameSpec {

    private static final String BITMAP_TYPE_NAME = "BITMAP";

    public SqlBitmapTypeNameSpec(SqlParserPos pos) {
        super(new SqlIdentifier(BITMAP_TYPE_NAME, pos), pos);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator) {
        return ((ExtendedRelTypeFactory) validator.getTypeFactory()).createBitmapType();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(BITMAP_TYPE_NAME);
    }

    @Override
    public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
        if (!(spec instanceof SqlBitmapTypeNameSpec)) {
            return litmus.fail("{} != {}", this, spec);
        }
        return litmus.succeed();
    }
}
