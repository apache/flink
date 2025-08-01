/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

/**
 * Flink modifications
 *
 * <p>Lines 71 ~ 76 to mitigate the impact of CALCITE-5570.
 *
 * <p>Lines 79 ~ 84 to mitigate the impact of CALCITE-5570.
 */
public class SqlMapTypeNameSpec extends SqlTypeNameSpec {

    private final SqlDataTypeSpec keyType;
    private final SqlDataTypeSpec valType;

    /**
     * Creates a {@code SqlMapTypeNameSpec}.
     *
     * @param keyType key type
     * @param valType value type
     * @param pos the parser position
     */
    public SqlMapTypeNameSpec(SqlDataTypeSpec keyType, SqlDataTypeSpec valType, SqlParserPos pos) {
        super(new SqlIdentifier(SqlTypeName.MAP.getName(), pos), pos);
        this.keyType = keyType;
        this.valType = valType;
    }

    public SqlDataTypeSpec getKeyType() {
        return keyType;
    }

    public SqlDataTypeSpec getValType() {
        return valType;
    }

    @Override
    public RelDataType deriveType(SqlValidator validator) {
        return validator
                .getTypeFactory()
                .createMapType(keyType.deriveType(validator), valType.deriveType(validator));
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("MAP");
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "<", ">");
        writer.sep(","); // configures the writer
        keyType.unparse(writer, leftPrec, rightPrec);
        // BEGIN FLINK MODIFICATION
        // Default is not null.
        // if (Boolean.TRUE.equals(keyType.getNullable())) {
        // writer.keyword("NULL");
        // }
        // END FLINK MODIFICATION
        writer.sep(",");
        valType.unparse(writer, leftPrec, rightPrec);
        // BEGIN FLINK MODIFICATION
        // Default is not null.
        // if (Boolean.TRUE.equals(valType.getNullable())) {
        // writer.keyword("NULL");
        // }
        // END FLINK MODIFICATION
        writer.endList(frame);
    }

    @Override
    public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
        if (!(spec instanceof SqlMapTypeNameSpec)) {
            return litmus.fail("{} != {}", this, spec);
        }
        SqlMapTypeNameSpec that = (SqlMapTypeNameSpec) spec;
        if (!this.keyType.equalsDeep(that.keyType, litmus)) {
            return litmus.fail("{} != {}", this, spec);
        }
        if (!this.valType.equalsDeep(that.valType, litmus)) {
            return litmus.fail("{} != {}", this, spec);
        }
        return litmus.succeed();
    }
}
