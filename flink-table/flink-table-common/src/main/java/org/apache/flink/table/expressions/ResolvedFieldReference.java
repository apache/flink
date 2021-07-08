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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.sources.FieldComputer;
import org.apache.flink.util.Preconditions;

/**
 * A reference to a field in an input which has been resolved.
 *
 * <p>Note: This interface is added as a temporary solution. It is used to keep api compatible for
 * {@link FieldComputer}. In the long term, this interface can be removed.
 */
@PublicEvolving
public class ResolvedFieldReference {

    private final String name;
    private final TypeInformation<?> resultType;
    private final int fieldIndex;

    public ResolvedFieldReference(String name, TypeInformation<?> resultType, int fieldIndex) {
        Preconditions.checkArgument(fieldIndex >= 0, "Index of field should be a positive number");
        this.name = Preconditions.checkNotNull(name, "Field name must not be null.");
        this.resultType =
                Preconditions.checkNotNull(resultType, "Field result type must not be null.");
        this.fieldIndex = fieldIndex;
    }

    public TypeInformation<?> resultType() {
        return resultType;
    }

    public String name() {
        return name;
    }

    public int fieldIndex() {
        return fieldIndex;
    }
}
