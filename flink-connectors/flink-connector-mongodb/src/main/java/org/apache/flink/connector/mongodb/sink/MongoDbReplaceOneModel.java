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

import org.apache.flink.annotation.Internal;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Internal Serializable wrapper class to MongoDB's {@link ReplaceOneModel}. */
@Internal
public final class MongoDbReplaceOneModel extends MongoDbWriteModel<ReplaceOneModel<Document>> {

    private transient ReplaceOneModel<Document> replaceOneModel;
    private final byte[] filter;
    private final byte[] replacement;
    private final boolean upsert;

    public MongoDbReplaceOneModel(ReplaceOneModel<Document> replaceOneModel) {
        this.replaceOneModel = replaceOneModel;
        this.filter =
                replaceOneModel
                        .getFilter()
                        .toBsonDocument()
                        .toJson()
                        .getBytes(StandardCharsets.UTF_8);
        this.replacement =
                replaceOneModel.getReplacement().toJson().getBytes(StandardCharsets.UTF_8);
        this.upsert = replaceOneModel.getReplaceOptions().isUpsert();
    }

    @Override
    public ReplaceOneModel<Document> getWriteModel() {
        return replaceOneModel;
    }

    @Override
    public int getSizeInBytes() {
        return filter.length + replacement.length;
    }

    private void writeObject(ObjectOutputStream outputStream) throws IOException {
        outputStream.defaultWriteObject();
    }

    private void readObject(ObjectInputStream inputStream)
            throws ClassNotFoundException, IOException {
        inputStream.defaultReadObject();
        this.replaceOneModel =
                new ReplaceOneModel<>(
                        Document.parse(new String(filter, StandardCharsets.UTF_8)),
                        Document.parse(new String(replacement, StandardCharsets.UTF_8)),
                        new ReplaceOptions().upsert(upsert));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoDbReplaceOneModel that = (MongoDbReplaceOneModel) o;
        return Arrays.equals(filter, that.filter) && Arrays.equals(replacement, that.replacement);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(filter);
        result = 31 * result + Arrays.hashCode(replacement);
        return result;
    }
}
