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

package org.apache.flink.connector.mongodb.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.sink.writer.MongoWriter;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;

import com.mongodb.client.model.WriteModel;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * MongoDB sink that requests multiple {@link WriteModel bulkRequests} against a cluster for each
 * incoming element. The following example shows how to create a MongoSink receiving records of
 * {@code Document} type.
 *
 * <pre>{@code
 * MongoSink<Document> sink = MongoSink.<Document>builder()
 *     .setUri("mongodb://user:password@127.0.0.1:27017")
 *     .setDatabase("db")
 *     .setCollection("coll")
 *     .setBulkFlushMaxActions(5)
 *     .setSerializationSchema(
 *         (doc, context) -> new InsertOneModel<>(doc.toBsonDocument()))
 *     .build();
 * }</pre>
 *
 * @param <IN> Type of the elements handled by this sink
 */
@PublicEvolving
public class MongoSink<IN> implements Sink<IN> {

    private static final long serialVersionUID = 1L;

    private final MongoConnectionOptions connectionOptions;
    private final MongoWriteOptions writeOptions;
    private final MongoSerializationSchema<IN> serializationSchema;

    MongoSink(
            MongoConnectionOptions connectionOptions,
            MongoWriteOptions writeOptions,
            MongoSerializationSchema<IN> serializationSchema) {
        this.connectionOptions = checkNotNull(connectionOptions);
        this.writeOptions = checkNotNull(writeOptions);
        this.serializationSchema = checkNotNull(serializationSchema);
    }

    public static <IN> MongoSinkBuilder<IN> builder() {
        return new MongoSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext context) {
        return new MongoWriter<>(
                connectionOptions,
                writeOptions,
                writeOptions.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE,
                context,
                serializationSchema);
    }
}
