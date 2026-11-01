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

package org.apache.flink.connector.datagen.table.types;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Composite {@link GeneratorFunction} that builds a {@link RowData} by invoking each per-field
 * generator with the current sequence index. With probability {@code nullRate}, an entire row is
 * replaced with {@code null}, matching the legacy behavior.
 */
@Internal
public class RowDataGenerator implements GeneratorFunction<Long, RowData> {

    private static final long serialVersionUID = 1L;

    private final GeneratorFunction<Long, ?>[] fieldGenerators;
    private final List<String> fieldNames;
    private final float nullRate;

    public RowDataGenerator(
            GeneratorFunction<Long, ?>[] fieldGenerators, List<String> fieldNames, float nullRate) {
        this.fieldGenerators = fieldGenerators;
        this.fieldNames = fieldNames;
        this.nullRate = nullRate;
    }

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        for (GeneratorFunction<Long, ?> generator : fieldGenerators) {
            generator.open(readerContext);
        }
    }

    @Override
    public RowData map(Long value) throws Exception {
        if (nullRate == 0f || ThreadLocalRandom.current().nextFloat() > nullRate) {
            GenericRowData row = new GenericRowData(fieldNames.size());
            for (int i = 0; i < fieldGenerators.length; i++) {
                row.setField(i, fieldGenerators[i].map(value));
            }
            return row;
        }
        return null;
    }
}
