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

package org.apache.flink.connector.mongodb.common.config;

import org.apache.flink.annotation.PublicEvolving;

import com.mongodb.ConnectionString;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The connection configuration class for MongoDB. */
@PublicEvolving
public class MongoConnectionOptions implements Serializable {

    private final String uri;
    private final String database;
    private final String collection;

    private MongoConnectionOptions(String uri, String database, String collection) {
        this.uri = checkNotNull(uri);
        this.database = checkNotNull(database);
        this.collection = checkNotNull(collection);
    }

    public String getUri() {
        return uri;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoConnectionOptions that = (MongoConnectionOptions) o;
        return Objects.equals(uri, that.uri)
                && Objects.equals(database, that.database)
                && Objects.equals(collection, that.collection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, database, collection);
    }

    public static MongoConnectionOptionsBuilder builder() {
        return new MongoConnectionOptionsBuilder();
    }

    /** Builder for {@link MongoConnectionOptions}. */
    public static class MongoConnectionOptionsBuilder {
        private String uri;
        private String database;
        private String collection;

        /**
         * Sets the connection string of MongoDB.
         *
         * @param uri connection string of MongoDB
         * @return this builder
         */
        public MongoConnectionOptionsBuilder setUri(String uri) {
            this.uri = new ConnectionString(uri).getConnectionString();
            return this;
        }

        /**
         * Sets the database of MongoDB.
         *
         * @param database the database to sink of MongoDB.
         * @return this builder
         */
        public MongoConnectionOptionsBuilder setDatabase(String database) {
            this.database = checkNotNull(database, "The database of MongoDB must not be null");
            return this;
        }

        /**
         * Sets the collection of MongoDB.
         *
         * @param collection the collection to sink of MongoDB.
         * @return this builder
         */
        public MongoConnectionOptionsBuilder setCollection(String collection) {
            this.collection =
                    checkNotNull(collection, "The collection of MongoDB must not be null");
            return this;
        }

        /**
         * Build the {@link MongoConnectionOptions}.
         *
         * @return a MongoConnectionOptions with the settings made for this builder.
         */
        public MongoConnectionOptions build() {
            return new MongoConnectionOptions(uri, database, collection);
        }
    }
}
