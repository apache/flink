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
import java.util.Map;
import java.util.Objects;

/** A Serializable wrapper class for the {@link ReplaceOneModel}. */
@Internal
public final class MongoReplaceOneModel extends MongoWriteModel<ReplaceOneModel<Document>> {

    private static final long serialVersionUID = -1504830587317551155L;

    private final Map<String, Object> filter;
    private final Map<String, Object> replacement;

    public MongoReplaceOneModel(
            final Map<String, Object> filter, final Map<String, Object> replacement) {
        this.replacement = replacement;
        this.filter = filter;
        this.createWriteModel();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoReplaceOneModel that = (MongoReplaceOneModel) o;
        return Objects.equals(filter, that.filter) && Objects.equals(replacement, that.replacement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, replacement);
    }

    private void readObject(ObjectInputStream inputStream)
            throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject();
        createWriteModel();
    }

    private void createWriteModel() {
        ReplaceOptions replaceOptions = new ReplaceOptions();
        replaceOptions.upsert(true);
        this.writeModel =
                new ReplaceOneModel<>(
                        new Document(filter), new Document(replacement), replaceOptions);
    }
}
