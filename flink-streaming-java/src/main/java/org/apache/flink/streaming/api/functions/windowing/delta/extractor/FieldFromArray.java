/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.windowing.delta.extractor;

import org.apache.flink.annotation.Internal;

import java.lang.reflect.Array;

/**
 * Extracts a single field out of an array.
 *
 * @param <OUT> The type of the extracted field.
 */
@Internal
public class FieldFromArray<OUT> implements Extractor<Object, OUT> {

    private static final long serialVersionUID = -5161386546695574359L;

    private int fieldId = 0;

    /** Extracts the first field (id 0) from the array. */
    public FieldFromArray() {
        // noting to do => will use default 0
    }

    /**
     * Extracts the field with the given id from the array.
     *
     * @param fieldId The id of the field which will be extracted from the array.
     */
    public FieldFromArray(int fieldId) {
        this.fieldId = fieldId;
    }

    @SuppressWarnings("unchecked")
    @Override
    public OUT extract(Object in) {
        return (OUT) Array.get(in, fieldId);
    }
}
