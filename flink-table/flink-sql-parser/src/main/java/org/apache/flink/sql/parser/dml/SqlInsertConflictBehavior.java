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

package org.apache.flink.sql.parser.dml;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * SQL parser enum for conflict resolution behaviors in INSERT INTO statements.
 *
 * <p>This enum is used by the SQL parser to represent the ON CONFLICT clause. It is converted to
 * {@link org.apache.flink.table.api.InsertConflictStrategy} during SQL-to-operation conversion.
 *
 * <p>Supported behaviors:
 *
 * <ul>
 *   <li>{@code ON CONFLICT DO ERROR} - Throw an exception on primary key constraint violation
 *   <li>{@code ON CONFLICT DO NOTHING} - Keep the first record, ignore subsequent conflicts
 *   <li>{@code ON CONFLICT DO DEDUPLICATE} - Maintain history for rollback (current behavior)
 * </ul>
 */
public enum SqlInsertConflictBehavior {
    /**
     * Throw an exception when multiple distinct records arrive for the same primary key after
     * watermark compaction.
     */
    ERROR,

    /** Keep the first record that arrives for a given primary key, discard subsequent conflicts. */
    NOTHING,

    /**
     * Maintain the full history of changes for each primary key to support rollback on retraction.
     * This is the current default behavior.
     */
    DEDUPLICATE;

    /**
     * Creates a parse-tree node representing an occurrence of this strategy at a particular
     * position in the parsed text.
     */
    public SqlLiteral symbol(SqlParserPos pos) {
        return SqlLiteral.createSymbol(this, pos);
    }
}
