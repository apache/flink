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

package org.apache.flink.connector.mongodb.common.utils;

import org.apache.flink.annotation.PublicEvolving;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonValue;

/** Constants for MongoDB. */
@PublicEvolving
public class MongoConstants {

    public static final String ID_FIELD = "_id";

    public static final String NAMESPACE_FIELD = "ns";

    public static final String KEY_FIELD = "key";

    public static final String MAX_FIELD = "max";

    public static final String MIN_FIELD = "min";

    public static final String UUID_FIELD = "uuid";

    public static final String SPLIT_KEYS_FIELD = "splitKeys";

    public static final String SHARD_FIELD = "shard";

    public static final String SHARDED_FIELD = "sharded";

    public static final String COUNT_FIELD = "count";

    public static final String SIZE_FIELD = "size";

    public static final String AVG_OBJ_SIZE_FIELD = "avgObjSize";

    public static final String DROPPED_FIELD = "dropped";

    public static final String ERROR_MESSAGE_FIELD = "errmsg";

    public static final String OK_FIELD = "ok";

    public static final BsonValue BSON_MIN_KEY = new BsonMinKey();

    public static final BsonValue BSON_MAX_KEY = new BsonMaxKey();

    public static final BsonDocument ID_HINT = new BsonDocument(ID_FIELD, new BsonInt32(1));

    private MongoConstants() {}
}
