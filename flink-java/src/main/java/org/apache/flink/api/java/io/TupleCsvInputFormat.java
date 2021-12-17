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

package org.apache.flink.api.java.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.core.fs.Path;

/** Input format that reads csv into tuples. */
@Internal
public class TupleCsvInputFormat<OUT> extends CsvInputFormat<OUT> {

    private static final long serialVersionUID = 1L;

    private TupleSerializerBase<OUT> tupleSerializer;

    public TupleCsvInputFormat(Path filePath, TupleTypeInfoBase<OUT> tupleTypeInfo) {
        this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, tupleTypeInfo);
    }

    public TupleCsvInputFormat(
            Path filePath,
            String lineDelimiter,
            String fieldDelimiter,
            TupleTypeInfoBase<OUT> tupleTypeInfo) {
        this(
                filePath,
                lineDelimiter,
                fieldDelimiter,
                tupleTypeInfo,
                createDefaultMask(tupleTypeInfo.getArity()));
    }

    public TupleCsvInputFormat(
            Path filePath, TupleTypeInfoBase<OUT> tupleTypeInfo, int[] includedFieldsMask) {
        this(
                filePath,
                DEFAULT_LINE_DELIMITER,
                DEFAULT_FIELD_DELIMITER,
                tupleTypeInfo,
                includedFieldsMask);
    }

    public TupleCsvInputFormat(
            Path filePath,
            String lineDelimiter,
            String fieldDelimiter,
            TupleTypeInfoBase<OUT> tupleTypeInfo,
            int[] includedFieldsMask) {
        super(filePath);
        boolean[] mask =
                (includedFieldsMask == null)
                        ? createDefaultMask(tupleTypeInfo.getArity())
                        : toBooleanMask(includedFieldsMask);
        configure(lineDelimiter, fieldDelimiter, tupleTypeInfo, mask);
    }

    public TupleCsvInputFormat(
            Path filePath, TupleTypeInfoBase<OUT> tupleTypeInfo, boolean[] includedFieldsMask) {
        this(
                filePath,
                DEFAULT_LINE_DELIMITER,
                DEFAULT_FIELD_DELIMITER,
                tupleTypeInfo,
                includedFieldsMask);
    }

    public TupleCsvInputFormat(
            Path filePath,
            String lineDelimiter,
            String fieldDelimiter,
            TupleTypeInfoBase<OUT> tupleTypeInfo,
            boolean[] includedFieldsMask) {
        super(filePath);
        configure(lineDelimiter, fieldDelimiter, tupleTypeInfo, includedFieldsMask);
    }

    private void configure(
            String lineDelimiter,
            String fieldDelimiter,
            TupleTypeInfoBase<OUT> tupleTypeInfo,
            boolean[] includedFieldsMask) {

        if (tupleTypeInfo.getArity() == 0) {
            throw new IllegalArgumentException("Tuple size must be greater than 0.");
        }

        if (includedFieldsMask == null) {
            includedFieldsMask = createDefaultMask(tupleTypeInfo.getArity());
        }

        tupleSerializer =
                (TupleSerializerBase<OUT>) tupleTypeInfo.createSerializer(new ExecutionConfig());

        setDelimiter(lineDelimiter);
        setFieldDelimiter(fieldDelimiter);

        Class<?>[] classes = new Class<?>[tupleTypeInfo.getArity()];

        for (int i = 0; i < tupleTypeInfo.getArity(); i++) {
            classes[i] = tupleTypeInfo.getTypeAt(i).getTypeClass();
        }

        setFieldsGeneric(includedFieldsMask, classes);
    }

    @Override
    public OUT fillRecord(OUT reuse, Object[] parsedValues) {
        return tupleSerializer.createOrReuseInstance(parsedValues, reuse);
    }
}
