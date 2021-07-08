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

import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;

import java.util.Objects;

/**
 * Represents type TIMESTAMP_LTZ(int) which is a synonym of type TIMESTAMP(int) WITH LOCAL TIME
 * ZONE.
 */
@Internal
public final class SqlTimestampLtzTypeNameSpec extends SqlBasicTypeNameSpec {

    // Type alias for unparsing.
    private final String typeAlias;
    private final SqlTypeName sqlTypeName;
    private final int precision;

    /**
     * Creates a {@code SqlTimestampLtzTypeNameSpec} instance.
     *
     * @param typeAlias Type alias of the alien system
     * @param typeName Type name the {@code typeAlias} implies as the (standard) basic type name
     * @param precision Type Precision
     * @param pos The parser position
     */
    public SqlTimestampLtzTypeNameSpec(
            String typeAlias, SqlTypeName typeName, int precision, SqlParserPos pos) {
        super(typeName, precision, pos);
        this.typeAlias = typeAlias;
        this.sqlTypeName = typeName;
        this.precision = precision;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(typeAlias);
        if (sqlTypeName.allowsPrec() && (precision >= 0)) {
            final SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
            writer.print(precision);
            writer.endList(frame);
        }
    }

    @Override
    public boolean equalsDeep(SqlTypeNameSpec node, Litmus litmus) {
        if (!(node instanceof SqlTimestampLtzTypeNameSpec)) {
            return litmus.fail("{} != {}", this, node);
        }
        SqlTimestampLtzTypeNameSpec that = (SqlTimestampLtzTypeNameSpec) node;
        if (!Objects.equals(this.typeAlias, that.typeAlias)) {
            return litmus.fail("{} != {}", this, node);
        }
        return super.equalsDeep(node, litmus);
    }
}
