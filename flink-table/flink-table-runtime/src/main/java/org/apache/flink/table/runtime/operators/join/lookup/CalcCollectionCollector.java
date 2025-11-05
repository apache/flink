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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;

/** A {@link Collector} used to store data after calc function. */
public class CalcCollectionCollector implements Collector<RowData> {

    private final RowDataSerializer lookupResultRowSerializer;

    private Collection<RowData> collection;

    public CalcCollectionCollector(RowDataSerializer lookupResultRowSerializer) {
        this.lookupResultRowSerializer = lookupResultRowSerializer;
    }

    public void reset() {
        this.collection = new ArrayList<>();
    }

    public Collection<RowData> getCollection() {
        return collection;
    }

    @Override
    public void collect(RowData record) {
        this.collection.add(lookupResultRowSerializer.copy(record));
    }

    @Override
    public void close() {}
}
