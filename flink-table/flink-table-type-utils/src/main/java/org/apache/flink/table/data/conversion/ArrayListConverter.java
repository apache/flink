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

package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;

/** Converter for {@link ArrayType} of {@link List} external type. */
@Internal
public class ArrayListConverter<E> implements DataStructureConverter<ArrayData, List<E>> {

    private static final long serialVersionUID = 1L;

    private final E[] arrayKind;

    private final ArrayObjectArrayConverter<E> elementsConverter;

    private ArrayListConverter(E[] arrayKind, ArrayObjectArrayConverter<E> elementsConverter) {
        this.arrayKind = arrayKind;
        this.elementsConverter = elementsConverter;
    }

    @Override
    public void open(ClassLoader classLoader) {
        elementsConverter.open(classLoader);
    }

    @Override
    public ArrayData toInternal(List<E> external) {
        return elementsConverter.toInternal(external.toArray(arrayKind));
    }

    @Override
    public List<E> toExternal(ArrayData internal) {
        return new ArrayList<>(Arrays.asList(elementsConverter.toExternal(internal)));
    }

    // --------------------------------------------------------------------------------------------
    // Factory method
    // --------------------------------------------------------------------------------------------

    public static ArrayListConverter<?> create(DataType dataType) {
        final DataType elementDataType = dataType.getChildren().get(0);
        return new ArrayListConverter<>(
                createObjectArrayKind(elementDataType.getConversionClass()),
                ArrayObjectArrayConverter.createForElement(elementDataType));
    }

    /** Creates the kind of array for {@link List#toArray(Object[])}. */
    private static Object[] createObjectArrayKind(Class<?> elementClazz) {
        // e.g. int[] is not a Object[]
        if (elementClazz.isPrimitive()) {
            return (Object[]) Array.newInstance(primitiveToWrapper(elementClazz), 0);
        }
        // e.g. int[][] and Integer[] are Object[]
        return (Object[]) Array.newInstance(elementClazz, 0);
    }
}
