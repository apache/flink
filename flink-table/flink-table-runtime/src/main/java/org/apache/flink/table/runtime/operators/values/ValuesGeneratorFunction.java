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
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;

/**
 * Returns the row of a {@code VALUES} clause for a row number. It instantiates the generated
 * function when it opens.
 */
@Internal
public class ValuesGeneratorFunction implements GeneratorFunction<Long, RowData> {

    private static final long serialVersionUID = 1L;

    private final GeneratedFunction<GeneratorFunction<Long, RowData>> generatedFunction;

    private transient GeneratorFunction<Long, RowData> function;

    public ValuesGeneratorFunction(
            GeneratedFunction<GeneratorFunction<Long, RowData>> generatedFunction) {
        this.generatedFunction = generatedFunction;
    }

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        function =
                generatedFunction.newInstance(
                        readerContext.getUserCodeClassLoader().asClassLoader());
    }

    @Override
    public RowData map(Long rowNumber) throws Exception {
        return function.map(rowNumber);
    }
}
