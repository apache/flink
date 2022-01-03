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

package org.apache.flink.table.test;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.ClassAssert;
import org.assertj.core.api.ListAssert;

import static org.assertj.core.api.Assertions.not;

/** Assertions for {@link DataType}. */
@Experimental
public class DataTypeAssert extends AbstractAssert<DataTypeAssert, DataType> {

    public DataTypeAssert(DataType dataType) {
        super(dataType, DataTypeAssert.class);
    }

    public LogicalTypeAssert asLogicalType() {
        isNotNull();
        return new LogicalTypeAssert(this.actual.getLogicalType());
    }

    public ClassAssert getConversionClass() {
        isNotNull();
        return new ClassAssert(this.actual.getConversionClass());
    }

    public ListAssert<DataType> getChildren() {
        isNotNull();
        return new ListAssert<>(this.actual.getChildren());
    }

    public DataTypeAssert hasConversionClass(Class<?> clazz) {
        isNotNull();
        getConversionClass().isEqualTo(clazz);
        return this;
    }

    public DataTypeAssert hasLogicalType(LogicalType logicalType) {
        isNotNull();
        asLogicalType().isEqualTo(logicalType);
        return this;
    }

    public DataTypeAssert isNullable() {
        satisfies(DataTypeConditions.NULLABLE);
        return this;
    }

    public DataTypeAssert isNotNullable() {
        satisfies(not(DataTypeConditions.NULLABLE));
        return this;
    }
}
