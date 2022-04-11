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

package org.apache.flink.streaming.connectors.kinesis.serialization;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A simple wrapper for using the {@link DeserializationSchema} with the {@link
 * KinesisDeserializationSchema} interface.
 *
 * @param <T> The type created by the deserialization schema.
 */
@Internal
public class KinesisDeserializationSchemaWrapper<T> implements KinesisDeserializationSchema<T> {
    private static final long serialVersionUID = 9143148962928375886L;

    private final DeserializationSchema<T> deserializationSchema;
    private final boolean useCollector;

    public KinesisDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {

        boolean useCollector = false;
        try {
            Class<? extends DeserializationSchema> deserializationClass =
                    deserializationSchema.getClass();
            useCollector =
                    !deserializationClass
                            .getMethod("deserialize", byte[].class, Collector.class)
                            .isDefault();
        } catch (NoSuchMethodException e) {
            // swallow the exception
        }
        this.useCollector = useCollector;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        this.deserializationSchema.open(context);
    }

    @Override
    public List<T> deserialize(
            byte[] recordValue,
            String partitionKey,
            String seqNum,
            long approxArrivalTimestamp,
            String stream,
            String shardId)
            throws IOException {

        List<T> out = new ArrayList<>();
        if (useCollector) {
            ListCollector<T> coll = new ListCollector<>(out);
            deserializationSchema.deserialize(recordValue, coll);
        } else {
            T val = deserializationSchema.deserialize(recordValue);
            if (val != null) {
                out.add(val);
            }
        }
        return out;
    }

    /*
    FLINK-4194

    @Override
    public boolean isEndOfStream(T nextElement) {
    	return deserializationSchema.isEndOfStream(nextElement);
    } */

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @VisibleForTesting
    protected boolean isUseCollector() {
        return useCollector;
    }
}
