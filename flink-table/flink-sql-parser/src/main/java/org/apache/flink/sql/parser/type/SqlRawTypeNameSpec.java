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
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.NlsString;

import java.util.Objects;

/**
 * Represents a raw type such as {@code RAW('org.my.Class', 'sW3Djsds...')}.
 *
 * <p>The raw type does not belong to standard SQL.
 */
@Internal
public final class SqlRawTypeNameSpec extends SqlTypeNameSpec {

    private static final String RAW_TYPE_NAME = "RAW";

    private final SqlNode className;

    private final SqlNode serializerString;

    public SqlRawTypeNameSpec(SqlNode className, SqlNode serializerString, SqlParserPos pos) {
        super(new SqlIdentifier(RAW_TYPE_NAME, pos), pos);
        this.className = className;
        this.serializerString = serializerString;
    }

    @Override
    public RelDataType deriveType(SqlValidator validator) {
        return ((ExtendedRelTypeFactory) validator.getTypeFactory())
                .createRawType(
                        ((NlsString) SqlLiteral.value(className)).getValue(),
                        ((NlsString) SqlLiteral.value(serializerString)).getValue());
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(RAW_TYPE_NAME);
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
        writer.sep(","); // configures the writer
        className.unparse(writer, leftPrec, rightPrec);
        writer.sep(",");
        serializerString.unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
    }

    @Override
    public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
        if (!(spec instanceof SqlRawTypeNameSpec)) {
            return litmus.fail("{} != {}", this, spec);
        }
        SqlRawTypeNameSpec that = (SqlRawTypeNameSpec) spec;
        if (!Objects.equals(this.className, that.className)) {
            return litmus.fail("{} != {}", this, spec);
        }
        if (!Objects.equals(this.serializerString, that.serializerString)) {
            return litmus.fail("{} != {}", this, spec);
        }
        return litmus.succeed();
    }
}
