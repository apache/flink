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

package org.apache.flink.table.api;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.Objects;

/** Column reference position class. */
@Internal
public class ColumnPosition {

    public static final ColumnPosition DEFAULT_POSIT =
            new ColumnPosition(null, ColumnPosition.Preposition.DEFAULT);
    public static final ColumnPosition FIRST_POSIT =
            new ColumnPosition(null, ColumnPosition.Preposition.FIRST);

    private final String referencedColumn;
    private final ColumnPosition.Preposition preposition;

    public static ColumnPosition of(
            String referencedColumn, ColumnPosition.Preposition preposition) {
        return new ColumnPosition(referencedColumn, preposition);
    }

    private ColumnPosition(String referencedColumn, ColumnPosition.Preposition preposition) {
        this.referencedColumn = referencedColumn;
        this.preposition = preposition;
    }

    public String getReferencedColumn() {
        return referencedColumn;
    }

    public ColumnPosition.Preposition getPreposition() {
        return preposition;
    }

    @Override
    public boolean equals(Object obj) {
        if (Objects.isNull(obj)) {
            return false;
        }
        if (obj instanceof ColumnPosition) {
            ColumnPosition otherColPosit = (ColumnPosition) obj;
            return Objects.equals(this.getPreposition(), otherColPosit.getPreposition())
                    && Objects.equals(
                            this.getReferencedColumn(), otherColPosit.getReferencedColumn());
        }
        return false;
    }

    /** Enum of preposition of the referenced column. */
    @Internal
    public enum Preposition {
        FIRST,
        AFTER,
        DEFAULT
    }

    /** Table column referenced position column not found exception class. */
    @Internal
    public static class ReferencedColumnNotFoundException extends RuntimeException
            implements Serializable {

        public ReferencedColumnNotFoundException(String currentColumn, String referencedColumn) {
            super(
                    String.format(
                            "The column '%s' referenced by column '%s' was not found.",
                            referencedColumn, currentColumn));
        }
    }
}
