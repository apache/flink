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

package org.apache.flink.connector.mongodb.table.serialization;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.connector.mongodb.table.converter.RowDataToBsonConverters;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.function.Function;

/** The serialization schema for flink {@link RowData} to serialize records into MongoDB. */
@Internal
public class MongoRowDataSerializationSchema implements MongoSerializationSchema<RowData> {

    private final RowDataToBsonConverters.RowDataToBsonConverter rowDataToBsonConverter;
    private final Function<RowData, BsonValue> createKey;

    public MongoRowDataSerializationSchema(
            RowDataToBsonConverters.RowDataToBsonConverter rowDataToBsonConverter,
            Function<RowData, BsonValue> createKey) {
        this.rowDataToBsonConverter = rowDataToBsonConverter;
        this.createKey = createKey;
    }

    @Override
    public WriteModel<BsonDocument> serialize(RowData element, MongoSinkContext context) {
        switch (element.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                return processUpsert(element);
            case UPDATE_BEFORE:
            case DELETE:
                return processDelete(element);
            default:
                throw new TableException("Unsupported message kind: " + element.getRowKind());
        }
    }

    private WriteModel<BsonDocument> processUpsert(RowData row) {
        final BsonDocument document = (BsonDocument) rowDataToBsonConverter.convert(row);
        final BsonValue key = createKey.apply(row);
        if (key != null) {
            BsonDocument filter = new BsonDocument("_id", key);
            // _id is immutable so we remove it here to prevent exception.
            document.remove("_id");
            BsonDocument update = new BsonDocument("$set", document);
            return new UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true));
        } else {
            return new InsertOneModel<>(document);
        }
    }

    private WriteModel<BsonDocument> processDelete(RowData row) {
        final BsonValue key = createKey.apply(row);
        BsonDocument filter = new BsonDocument("_id", key);
        return new DeleteOneModel<>(filter);
    }
}
