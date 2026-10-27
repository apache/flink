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

package org.apache.flink.table.runtime.operators.values;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

/**
 * A {@link DataGeneratorSource} over the rows of a {@code VALUES} clause in streaming mode. It
 * checkpoints the number of the next row to emit, so a restored job emits only the rows it has not
 * emitted yet.
 *
 * <p>Unlike {@link DataGeneratorSource}, it is unbounded, so that a pipeline in {@code AUTOMATIC}
 * runtime mode, e.g. one that is converted to a {@code DataStream}, keeps running in streaming
 * mode.
 */
@Internal
public class ValuesSource extends DataGeneratorSource<RowData> {

    private static final long serialVersionUID = 1L;

    public ValuesSource(
            GeneratedFunction<GeneratorFunction<Long, RowData>> generatedFunction,
            InternalTypeInfo<RowData> producedType,
            int numRows) {
        super(new ValuesGeneratorFunction(generatedFunction), numRows, producedType);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }
}
