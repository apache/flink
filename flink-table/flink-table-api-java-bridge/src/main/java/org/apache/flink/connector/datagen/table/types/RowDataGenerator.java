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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** Data generator for Flink's internal {@link RowData} type. */
@Internal
public class RowDataGenerator implements DataGenerator<RowData> {

    private static final long serialVersionUID = 1L;

    private final DataGenerator<?>[] fieldGenerators;
    private final List<String> fieldNames;
    private final float nullRate;

    public RowDataGenerator(
            DataGenerator<?>[] fieldGenerators, List<String> fieldNames, float nullRate) {
        this.fieldGenerators = fieldGenerators;
        this.fieldNames = fieldNames;
        this.nullRate = nullRate;
    }

    @Override
    public void open(
            String name, FunctionInitializationContext context, RuntimeContext runtimeContext)
            throws Exception {
        for (int i = 0; i < fieldGenerators.length; i++) {
            fieldGenerators[i].open(fieldNames.get(i), context, runtimeContext);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        for (DataGenerator<?> generator : fieldGenerators) {
            generator.snapshotState(context);
        }
    }

    @Override
    public boolean hasNext() {
        for (DataGenerator<?> generator : fieldGenerators) {
            if (!generator.hasNext()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public RowData next() {
        if (nullRate == 0f || ThreadLocalRandom.current().nextFloat() > nullRate) {
            GenericRowData row = new GenericRowData(fieldNames.size());
            for (int i = 0; i < fieldGenerators.length; i++) {
                row.setField(i, fieldGenerators[i].next());
            }
            return row;
        }
        return null;
    }
}
