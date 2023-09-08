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

package org.apache.flink.python.util;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.Function;

/** Utility class for using DataStream connectors in Python. */
public class PythonConnectorUtils {

    /**
     * Creates a selector that returns the first column of a row, and cast it to {@code clazz}.
     * {@code T} should be a sub interface of {@link Function}, which accepts a {@link Row}.
     *
     * @param clazz The desired selector class to cast to, e.g. TopicSelector.class for Kafka.
     * @param <T> An interface
     */
    @SuppressWarnings("unchecked")
    public static <T> T createFirstColumnTopicSelector(Class<T> clazz) {
        return (T)
                Proxy.newProxyInstance(
                        clazz.getClassLoader(),
                        new Class[] {clazz},
                        new FirstColumnTopicSelectorInvocationHandler());
    }

    /** The serializable {@link InvocationHandler} as the proxy for first column selector. */
    public static class FirstColumnTopicSelectorInvocationHandler
            implements InvocationHandler, Serializable {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Preconditions.checkArgument(method.getName().equals("apply"));
            Preconditions.checkArgument(args.length == 1);
            Preconditions.checkArgument(args[0] instanceof Row);
            Row row = (Row) args[0];
            Preconditions.checkArgument(row.getArity() >= 1);
            return row.getField(0);
        }
    }

    /**
     * A {@link SerializationSchema} for {@link Row} that only serialize the second column using a
     * wrapped {@link SerializationSchema} for {@link T}.
     *
     * @param <T> The actual data type wrapped in the Row.
     */
    public static class SecondColumnSerializationSchema<T> implements SerializationSchema<Row> {

        private static final long serialVersionUID = 1L;

        private final SerializationSchema<T> wrappedSchema;

        /**
         * The constructor.
         *
         * @param wrappedSchema The {@link SerializationSchema} to serialize {@link T} objects.
         */
        public SecondColumnSerializationSchema(SerializationSchema<T> wrappedSchema) {
            this.wrappedSchema = wrappedSchema;
        }

        @Override
        public void open(InitializationContext context) throws Exception {
            wrappedSchema.open(context);
        }

        @SuppressWarnings("unchecked")
        @Override
        public byte[] serialize(Row row) {
            Preconditions.checkArgument(row.getArity() >= 2);
            return wrappedSchema.serialize((T) row.getField(1));
        }
    }

    /** A {@link ProcessFunction} that convert {@link Row} to {@link RowData}. */
    public static class RowRowMapper extends ProcessFunction<Row, RowData> {

        private static final long serialVersionUID = 1L;
        private final DataType dataType;
        private transient RowRowConverter converter;

        public RowRowMapper(DataType dataType) {
            this.dataType = dataType;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            converter = RowRowConverter.create(dataType);
            converter.open(getRuntimeContext().getUserCodeClassLoader());
        }

        @Override
        public void processElement(
                Row row, ProcessFunction<Row, RowData>.Context ctx, Collector<RowData> out)
                throws Exception {
            out.collect(converter.toInternal(row));
        }
    }
}
