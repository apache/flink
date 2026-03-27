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

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.sql.parser.SqlParseUtils;

import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.SqlUnknownLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nullable;

/** Start mode configuration for materialized tables. */
@Internal
public class SqlStartMode {
    private final SqlStartModeKind kind;
    private final @Nullable SqlTimestampLiteral timestampLiteral;
    private final @Nullable SqlIntervalLiteral intervalLiteral;

    public SqlStartModeKind getKind() {
        return kind;
    }

    @Nullable
    public SqlTimestampLiteral getTimestampLiteral() {
        return timestampLiteral;
    }

    @Nullable
    public SqlIntervalLiteral getIntervalLiteral() {
        return intervalLiteral;
    }

    public enum SqlStartModeKind {
        FROM_BEGINNING,
        FROM_NOW,
        FROM_TIMESTAMP,
        RESUME_OR_FROM_BEGINNING,
        RESUME_OR_FROM_NOW,
        RESUME_OR_FROM_TIMESTAMP;

        /**
         * Creates a parse-tree node representing an occurrence of this keyword at a particular
         * position in the parsed text.
         */
        public SqlLiteral symbol(SqlParserPos pos) {
            return SqlLiteral.createSymbol(this, pos);
        }

        public boolean isFromNow() {
            return this == FROM_NOW || this == RESUME_OR_FROM_NOW;
        }

        public boolean isFromTimestamp() {
            return this == FROM_TIMESTAMP || this == RESUME_OR_FROM_TIMESTAMP;
        }

        public boolean isCreateOrAlterOption() {
            return this == RESUME_OR_FROM_BEGINNING
                    || this == RESUME_OR_FROM_NOW
                    || this == RESUME_OR_FROM_TIMESTAMP;
        }
    }

    public SqlStartMode(SqlLiteral kind, @Nullable SqlLiteral literal, SqlParserPos pos) {
        this.kind = SqlParseUtils.extractEnum(kind, SqlStartModeKind.class);
        if (literal == null) {
            this.intervalLiteral = null;
            this.timestampLiteral = null;
        } else if (literal instanceof SqlIntervalLiteral) {
            this.intervalLiteral = (SqlIntervalLiteral) literal;
            this.timestampLiteral = null;
        } else {
            this.intervalLiteral = null;

            final SqlUnknownLiteral unknownLiteral = (SqlUnknownLiteral) literal;
            this.timestampLiteral =
                    (SqlTimestampLiteral)
                            unknownLiteral.resolve(SqlTypeName.lookup(unknownLiteral.tag));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(kind.name());

        if (kind.isFromNow() && intervalLiteral != null) {
            sb.append("(").append(intervalLiteral).append(")");
        } else if (kind.isFromTimestamp() && timestampLiteral != null) {
            sb.append("(").append(timestampLiteral.toString().replace("_", " ")).append(")");
        }
        return sb.toString();
    }
}
