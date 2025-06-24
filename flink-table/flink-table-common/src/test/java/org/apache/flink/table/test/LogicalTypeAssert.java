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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.LogicalTypesTest;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.InstantiationUtil;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.ListAssert;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.not;

/** Assertions for {@link LogicalType}. */
@Experimental
public class LogicalTypeAssert extends AbstractAssert<LogicalTypeAssert, LogicalType> {

    public LogicalTypeAssert(LogicalType logicalType) {
        super(logicalType, LogicalTypeAssert.class);
    }

    // --- Generic LogicalType properties

    public LogicalTypeAssert isNullable() {
        isNotNull();
        satisfies(LogicalTypeConditions.NULLABLE);
        return myself;
    }

    public LogicalTypeAssert isNotNullable() {
        isNotNull();
        satisfies(not(LogicalTypeConditions.NULLABLE));
        return myself;
    }

    public ListAssert<LogicalType> getChildren() {
        isNotNull();
        return new ListAssert<>(this.actual.getChildren());
    }

    public LogicalTypeAssert hasExactlyChildren(LogicalType... children) {
        isNotNull();
        getChildren().containsExactly(children);
        return myself;
    }

    public LogicalTypeAssert hasSerializableString(String serializableString) {
        isNotNull();
        assertThat(this.actual.asSerializableString()).isEqualTo(serializableString);
        return myself;
    }

    public LogicalTypeAssert hasNoSerializableString() {
        isNotNull();
        assertThatThrownBy(this.actual::asSerializableString).isInstanceOf(TableException.class);
        return myself;
    }

    public LogicalTypeAssert hasSummaryString(String summaryString) {
        isNotNull();
        assertThat(this.actual.asSummaryString()).isEqualTo(summaryString);
        return myself;
    }

    public LogicalTypeAssert supportsInputConversion(Class<?> clazz) {
        isNotNull();
        assertThat(this.actual.supportsInputConversion(clazz)).isTrue();
        return myself;
    }

    public LogicalTypeAssert doesNotSupportInputConversion(Class<?> clazz) {
        isNotNull();
        assertThat(this.actual.supportsInputConversion(clazz)).isFalse();
        return myself;
    }

    public LogicalTypeAssert supportsOutputConversion(Class<?> clazz) {
        isNotNull();
        assertThat(this.actual.supportsOutputConversion(clazz)).isTrue();
        return myself;
    }

    public LogicalTypeAssert doesNotSupportOutputConversion(Class<?> clazz) {
        isNotNull();
        assertThat(this.actual.supportsOutputConversion(clazz)).isFalse();
        return myself;
    }

    public LogicalTypeAssert isJavaSerializable() {
        isNotNull();
        try {
            assertThat(
                            InstantiationUtil.<LogicalType>deserializeObject(
                                    InstantiationUtil.serializeObject(this.actual),
                                    LogicalTypesTest.class.getClassLoader()))
                    .isEqualTo(this.actual);
        } catch (IOException | ClassNotFoundException e) {
            fail(
                    "Error when trying to serialize logical type "
                            + this.actual.asSummaryString()
                            + " to string",
                    e);
        }
        return myself;
    }

    // --- Subtypes asserts

    public LogicalTypeAssert isDecimalType() {
        isNotNull();
        assertThat(this.actual.getTypeRoot()).isEqualTo(LogicalTypeRoot.DECIMAL);
        isInstanceOf(DecimalType.class);
        return myself;
    }

    // --- Subtype properties

    public LogicalTypeAssert hasPrecision(int precision) {
        assertThat(LogicalTypeChecks.getPrecision(this.actual)).isEqualTo(precision);
        return myself;
    }

    public LogicalTypeAssert hasScale(int scale) {
        assertThat(LogicalTypeChecks.getScale(this.actual)).isEqualTo(scale);
        return myself;
    }

    public LogicalTypeAssert hasPrecisionAndScale(int precision, int scale) {
        return hasPrecision(precision).hasScale(scale);
    }
}
