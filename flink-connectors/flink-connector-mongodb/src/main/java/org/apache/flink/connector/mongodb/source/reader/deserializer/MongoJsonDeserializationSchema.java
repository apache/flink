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

package org.apache.flink.connector.mongodb.source.reader.deserializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.bson.BsonDocument;
import org.bson.json.JsonMode;

import java.util.Optional;

/**
 * A schema bridge for deserializing the MongoDB's {@code BsonDocument} to MongoDB's {@link
 * JsonMode}'s RELAXED Json string.
 */
@PublicEvolving
public class MongoJsonDeserializationSchema implements MongoDeserializationSchema<String> {

    @Override
    public String deserialize(BsonDocument document) {
        return Optional.ofNullable(document).map(BsonDocument::toJson).orElse(null);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
