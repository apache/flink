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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

/** Factory for creating configured instances of {@link CsvTableSource} in a stream environment. */
@PublicEvolving
public class CsvAppendTableSourceFactory extends CsvTableSourceFactoryBase
        implements StreamTableSourceFactory<Row> {

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>(super.requiredContext());
        context.put(UPDATE_MODE, UPDATE_MODE_VALUE_APPEND);
        return context;
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        return createTableSource(true, properties);
    }
}
