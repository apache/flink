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

import org.apache.flink.util.Preconditions;

/** A builder class for creating {@link MongoClientProvider}. */
public class MongoColloctionProviders {

    public static Builder getBuilder() {
        return new Builder();
    }

    /** */
    public static class Builder {

        private String connectionString;

        private String database;

        private String collection;

        public Builder connectionString(String connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder collection(String collection) {
            this.collection = collection;
            return this;
        }

        public MongoClientProvider build() {
            Preconditions.checkNotNull(connectionString, "Connection string must not be null");
            Preconditions.checkNotNull(database, "Database must not be null");
            Preconditions.checkNotNull(collection, "Collection must not be null");
            return new MongoSingleCollectionProvider(connectionString, database, collection);
        }
    }
}
