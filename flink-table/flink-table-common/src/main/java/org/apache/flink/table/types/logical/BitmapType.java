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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.bitmap.Bitmap;
import org.apache.flink.types.bitmap.RoaringBitmapData;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Data type of bitmap data.
 *
 * <p>This type supports storing 32-bit integers in a compressed form. This can be useful for
 * efficiently representing and querying large sets of integers.
 *
 * <p>The serializable string representation of this type is {@code BITMAP}.
 */
@PublicEvolving
public final class BitmapType extends LogicalType {

    private static final long serialVersionUID = 1L;

    private static final Set<String> INPUT_OUTPUT_CONVERSION =
            conversionSet(Bitmap.class.getName(), RoaringBitmapData.class.getName());

    public BitmapType(boolean isNullable) {
        super(isNullable, LogicalTypeRoot.BITMAP);
    }

    public BitmapType() {
        this(true);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new BitmapType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability("BITMAP");
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return Bitmap.class;
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
