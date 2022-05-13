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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/** An output format that adds records to a collection. */
@PublicEvolving
public class LocalCollectionOutputFormat<T> extends RichOutputFormat<T>
        implements InputTypeConfigurable {

    private static final long serialVersionUID = 1L;

    private static final Map<Integer, Collection<?>> RESULT_HOLDER =
            new HashMap<Integer, Collection<?>>();

    private transient ArrayList<T> taskResult;

    private TypeSerializer<T> typeSerializer;

    private int id;

    public LocalCollectionOutputFormat(Collection<T> out) {
        synchronized (RESULT_HOLDER) {
            this.id = generateRandomId();
            RESULT_HOLDER.put(this.id, out);
        }
    }

    private int generateRandomId() {
        int num = (int) (Math.random() * Integer.MAX_VALUE);
        while (RESULT_HOLDER.containsKey(num)) {
            num = (int) (Math.random() * Integer.MAX_VALUE);
        }
        return num;
    }

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.taskResult = new ArrayList<T>();
    }

    @Override
    public void writeRecord(T record) throws IOException {
        T recordCopy = this.typeSerializer.createInstance();
        recordCopy = this.typeSerializer.copy(record, recordCopy);
        this.taskResult.add(recordCopy);
    }

    @Override
    public void close() throws IOException {
        synchronized (RESULT_HOLDER) {
            @SuppressWarnings("unchecked")
            Collection<T> result = (Collection<T>) RESULT_HOLDER.get(this.id);
            result.addAll(this.taskResult);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        this.typeSerializer = (TypeSerializer<T>) type.createSerializer(executionConfig);
    }
}
