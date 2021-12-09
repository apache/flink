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

package org.apache.flink.mongodb.internal.connection;

import org.apache.flink.annotation.Internal;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;

import java.io.Serializable;

/** Provided for initiate and recreate {@link MongoClient}. */
@Internal
public interface MongoClientProvider extends Serializable {

    /**
     * Create one or get the current {@link MongoClient}.
     *
     * @return Current {@link MongoClient}.
     */
    MongoClient getClient();

    /**
     * Get the default database.
     *
     * @return Current {@link MongoDatabase}.
     */
    MongoDatabase getDefaultDatabase();

    /**
     * Get the default collection.
     *
     * @return Current {@link MongoCollection}.
     */
    MongoCollection<Document> getDefaultCollection();

    /**
     * Recreate a client. Used typically when a connection is timed out or lost.
     *
     * @return A new {@link MongoClient}.
     */
    MongoClient recreateClient();

    /** Close the underlying MongoDB connection. */
    void close();
}
