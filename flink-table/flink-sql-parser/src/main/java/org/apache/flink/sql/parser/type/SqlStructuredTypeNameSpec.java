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

import org.apache.flink.table.calcite.ExtendedRelTypeFactory;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.stream.Collectors;

/** A SQL type name specification of STRUCTURED type. */
public class SqlStructuredTypeNameSpec extends SqlTypeNameSpec {

    private final SqlNode className;
    private final List<SqlIdentifier> fieldNames;
    private final List<SqlDataTypeSpec> fieldTypes;
    private final List<SqlCharStringLiteral> comments;

    public SqlStructuredTypeNameSpec(
            SqlParserPos pos,
            SqlNode className,
            List<SqlIdentifier> fieldNames,
            List<SqlDataTypeSpec> fieldTypes,
            List<SqlCharStringLiteral> comments) {
        super(new SqlIdentifier(SqlTypeName.STRUCTURED.getName(), pos), pos);
        this.className = className;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.comments = comments;
    }

    public List<SqlIdentifier> getFieldNames() {
        return fieldNames;
    }

    public List<SqlDataTypeSpec> getFieldTypes() {
        return fieldTypes;
    }

    public List<SqlCharStringLiteral> getComments() {
        return comments;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print("STRUCTURED");
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "<", ">");
        writer.sep(","); // configures the writer
        className.unparse(writer, leftPrec, rightPrec);
        int i = 0;
        for (Pair<SqlIdentifier, SqlDataTypeSpec> p : Pair.zip(fieldNames, fieldTypes)) {
            assert p.left != null;
            assert p.right != null;
            writer.sep(",");
            p.left.unparse(writer, 0, 0);
            p.right.unparse(writer, leftPrec, rightPrec);
            if (p.right.getNullable() != null && !p.right.getNullable()) {
                writer.keyword("NOT NULL");
            }
            if (comments.get(i) != null) {
                comments.get(i).unparse(writer, leftPrec, rightPrec);
            }
            i += 1;
        }
        writer.endList(frame);
    }

    @Override
    public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
        if (!(spec instanceof SqlStructuredTypeNameSpec)) {
            return litmus.fail("{} != {}", this, spec);
        }
        final SqlStructuredTypeNameSpec that = (SqlStructuredTypeNameSpec) spec;
        if (!this.className.equalsDeep(that.className, litmus)) {
            return litmus.fail("{} != {}", this, spec);
        }
        if (this.fieldNames.size() != that.fieldNames.size()) {
            return litmus.fail("{} != {}", this, spec);
        }
        for (int i = 0; i < fieldNames.size(); i++) {
            if (!this.fieldNames.get(i).equalsDeep(that.fieldNames.get(i), litmus)) {
                return litmus.fail("{} != {}", this, spec);
            }
        }
        if (this.fieldTypes.size() != that.fieldTypes.size()) {
            return litmus.fail("{} != {}", this, spec);
        }
        for (int i = 0; i < fieldTypes.size(); i++) {
            if (!this.fieldTypes.get(i).equalsDeep(that.fieldTypes.get(i), litmus)) {
                return litmus.fail("{} != {}", this, spec);
            }
        }
        return litmus.succeed();
    }

    @Override
    @SuppressWarnings("DataFlowIssue")
    public RelDataType deriveType(SqlValidator sqlValidator) {
        final ExtendedRelTypeFactory typeFactory =
                (ExtendedRelTypeFactory) sqlValidator.getTypeFactory();
        return typeFactory.createStructuredType(
                ((NlsString) SqlLiteral.value(className)).getValue(),
                fieldTypes.stream()
                        .map(dt -> dt.deriveType(sqlValidator))
                        .collect(Collectors.toList()),
                fieldNames.stream().map(SqlIdentifier::toString).collect(Collectors.toList()));
    }
}
