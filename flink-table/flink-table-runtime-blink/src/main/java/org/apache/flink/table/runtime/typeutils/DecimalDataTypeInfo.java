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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.DecimalData;

import java.util.Arrays;

/** TypeInformation for {@link DecimalData}. */
@Internal
public class DecimalDataTypeInfo extends TypeInformation<DecimalData> {

    private static final long serialVersionUID = 1L;

    public static DecimalDataTypeInfo of(int precision, int scale) {
        return new DecimalDataTypeInfo(precision, scale);
    }

    private final int precision;

    private final int scale;

    public DecimalDataTypeInfo(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public boolean isBasicType() {
        return true;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<DecimalData> getTypeClass() {
        return DecimalData.class;
    }

    @Override
    public boolean isKeyType() {
        return true;
    }

    @Override
    public TypeSerializer<DecimalData> createSerializer(ExecutionConfig config) {
        return new DecimalDataSerializer(precision, scale);
    }

    @Override
    public String toString() {
        return String.format("Decimal(%d,%d)", precision, scale);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DecimalDataTypeInfo)) {
            return false;
        }
        DecimalDataTypeInfo that = (DecimalDataTypeInfo) obj;
        return this.precision == that.precision && this.scale == that.scale;
    }

    @Override
    public int hashCode() {
        int h0 = this.getClass().getCanonicalName().hashCode();
        return Arrays.hashCode(new int[] {h0, precision, scale});
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof DecimalDataTypeInfo;
    }

    public int precision() {
        return precision;
    }

    public int scale() {
        return scale;
    }
}
