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

import com.mongodb.client.model.InsertOneModel;
import org.bson.Document;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Internal Serializable wrapper class to MongoDB's {@link InsertOneModel}. */
@Internal
public class MongoDbInsertOneModel extends MongoDbWriteModel<InsertOneModel<Document>> {

    private transient InsertOneModel<Document> insertOneModel;
    private final byte[] document;

    public MongoDbInsertOneModel(InsertOneModel<Document> insertOneModel) {
        this.insertOneModel = insertOneModel;
        this.document = insertOneModel.getDocument().toJson().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public InsertOneModel<Document> getWriteModel() {
        return insertOneModel;
    }

    @Override
    public int getSizeInBytes() {
        return document.length;
    }

    private void writeObject(ObjectOutputStream outputStream) throws IOException {
        outputStream.defaultWriteObject();
    }

    private void readObject(ObjectInputStream inputStream)
            throws ClassNotFoundException, IOException {
        inputStream.defaultReadObject();
        insertOneModel =
                new InsertOneModel<>(Document.parse(new String(document, StandardCharsets.UTF_8)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoDbInsertOneModel that = (MongoDbInsertOneModel) o;
        return Arrays.equals(document, that.document);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(document);
    }
}
