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

package org.apache.flink.connector.mongodb.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.sink.MongoDbInsertOneModel;
import org.apache.flink.connector.mongodb.sink.MongoDbReplaceOneModel;
import org.apache.flink.connector.mongodb.sink.MongoDbWriteModel;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Utils to serialize {@link MongoDbWriteModel}. */
@Internal
public class SerializationUtils {

    public static void serializeMongoDbWriteModel(
            MongoDbWriteModel<? extends WriteModel<Document>> mongoDbWriteModel,
            DataOutputStream out)
            throws IOException {
        if (mongoDbWriteModel instanceof MongoDbInsertOneModel) {
            serializeMongoDbInsertOneModel((MongoDbInsertOneModel) mongoDbWriteModel, out);
        } else if (mongoDbWriteModel instanceof MongoDbReplaceOneModel) {
            serializeMongoDbReplaceOneModel((MongoDbReplaceOneModel) mongoDbWriteModel, out);
        }
    }

    private static void serializeMongoDbInsertOneModel(
            MongoDbInsertOneModel mongoDbInsertOneModel, DataOutputStream out) throws IOException {
        out.writeUTF(MongoDbWriteModelType.INSERT_ONE.name());
        InsertOneModel<Document> insertOneModel = mongoDbInsertOneModel.getWriteModel();
        byte[] document = insertOneModel.getDocument().toJson().getBytes(StandardCharsets.UTF_8);
        out.writeInt(document.length);
        out.write(document);
    }

    private static void serializeMongoDbReplaceOneModel(
            MongoDbReplaceOneModel mongoDbReplaceOneModel, DataOutputStream out)
            throws IOException {
        out.writeUTF(MongoDbWriteModelType.REPLACE_ONE.name());
        ReplaceOneModel<Document> replaceOneModel = mongoDbReplaceOneModel.getWriteModel();
        byte[] filter =
                replaceOneModel
                        .getFilter()
                        .toBsonDocument()
                        .toJson()
                        .getBytes(StandardCharsets.UTF_8);
        byte[] replacement =
                replaceOneModel.getReplacement().toJson().getBytes(StandardCharsets.UTF_8);
        out.writeInt(filter.length);
        out.write(filter);
        out.writeInt(replacement.length);
        out.write(replacement);
        out.writeBoolean(replaceOneModel.getReplaceOptions().isUpsert());
    }

    public static MongoDbWriteModel<? extends WriteModel<Document>> deserializeMongoDbWriteModel(
            DataInputStream in) throws IOException {
        MongoDbWriteModelType type = MongoDbWriteModelType.valueOf(in.readUTF());
        switch (type) {
            case INSERT_ONE:
                return deserializeMongoDbInsertOneModel(in);
            case REPLACE_ONE:
                return deserializeMongoDbReplaceOneModel(in);
            default:
                throw new IllegalArgumentException("Unknown MongoDbWriteModelType " + type);
        }
    }

    private static MongoDbInsertOneModel deserializeMongoDbInsertOneModel(DataInputStream in)
            throws IOException {
        int documentLength = in.readInt();
        byte[] document = new byte[documentLength];
        in.read(document);
        return new MongoDbInsertOneModel(
                new InsertOneModel<>(Document.parse(new String(document, StandardCharsets.UTF_8))));
    }

    private static MongoDbReplaceOneModel deserializeMongoDbReplaceOneModel(DataInputStream in)
            throws IOException {
        int filterLength = in.readInt();
        byte[] filter = new byte[filterLength];
        in.read(filter);
        int replacementLength = in.readInt();
        byte[] replacement = new byte[replacementLength];
        in.read(replacement);
        boolean upsert = in.readBoolean();
        return new MongoDbReplaceOneModel(
                new ReplaceOneModel<>(
                        Document.parse(new String(filter, StandardCharsets.UTF_8)),
                        Document.parse(new String(replacement, StandardCharsets.UTF_8)),
                        new ReplaceOptions().upsert(upsert)));
    }
}
