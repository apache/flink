/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

/**
 * A extended sql type name specification of collection type, different with {@link
 * SqlCollectionTypeNameSpec}, we support NULL or NOT NULL suffix for the element type name(this
 * syntax does not belong to standard SQL).
 */
public class ExtendedSqlCollectionTypeNameSpec extends SqlCollectionTypeNameSpec {
    private final boolean elementNullable;
    private final SqlTypeName collectionTypeName;
    private final boolean unparseAsStandard;

    /**
     * Creates a {@code ExtendedSqlCollectionTypeNameSpec}.
     *
     * @param elementTypeName element type name specification
     * @param elementNullable flag indicating if the element type is nullable
     * @param collectionTypeName collection type name
     * @param unparseAsStandard if we should unparse the collection type as standard SQL style
     * @param pos the parser position
     */
    public ExtendedSqlCollectionTypeNameSpec(
            SqlTypeNameSpec elementTypeName,
            boolean elementNullable,
            SqlTypeName collectionTypeName,
            boolean unparseAsStandard,
            SqlParserPos pos) {
        super(elementTypeName, collectionTypeName, pos);
        this.elementNullable = elementNullable;
        this.collectionTypeName = collectionTypeName;
        this.unparseAsStandard = unparseAsStandard;
    }

    public boolean elementNullable() {
        return elementNullable;
    }

    public SqlTypeName getCollectionTypeName() {
        return collectionTypeName;
    }

    public boolean unparseAsStandard() {
        return unparseAsStandard;
    }

    @Override
    public RelDataType deriveType(SqlValidator validator) {
        RelDataType elementType = getElementTypeName().deriveType(validator);
        elementType =
                validator.getTypeFactory().createTypeWithNullability(elementType, elementNullable);
        return createCollectionType(elementType, validator.getTypeFactory());
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (unparseAsStandard) {
            this.getElementTypeName().unparse(writer, leftPrec, rightPrec);
            // Default is nullable.
            if (!elementNullable) {
                writer.keyword("NOT NULL");
            }
            writer.keyword(collectionTypeName.name());
        } else {
            writer.keyword(collectionTypeName.name());
            SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "<", ">");

            getElementTypeName().unparse(writer, leftPrec, rightPrec);
            // Default is nullable.
            if (!elementNullable) {
                writer.keyword("NOT NULL");
            }
            writer.endList(frame);
        }
    }

    @Override
    public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
        if (!(spec instanceof ExtendedSqlCollectionTypeNameSpec)) {
            return litmus.fail("{} != {}", this, spec);
        }
        ExtendedSqlCollectionTypeNameSpec that = (ExtendedSqlCollectionTypeNameSpec) spec;
        if (this.elementNullable != that.elementNullable) {
            return litmus.fail("{} != {}", this, spec);
        }
        return super.equalsDeep(spec, litmus);
    }

    // ~ Tools ------------------------------------------------------------------

    /**
     * Create collection data type.
     *
     * @param elementType Type of the collection element
     * @param typeFactory Type factory
     * @return The collection data type, or throw exception if the collection type name does not
     *     belong to {@code SqlTypeName} enumerations
     */
    private RelDataType createCollectionType(
            RelDataType elementType, RelDataTypeFactory typeFactory) {
        switch (collectionTypeName) {
            case MULTISET:
                return typeFactory.createMultisetType(elementType, -1);
            case ARRAY:
                return typeFactory.createArrayType(elementType, -1);

            default:
                throw Util.unexpected(collectionTypeName);
        }
    }
}
