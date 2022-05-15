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

import org.apache.flink.annotation.PublicEvolving;

/** A {@link RuntimeException} wrapper indicating the exception was thrown from the MongoDB Sink. */
@PublicEvolving
public class MongoDbException extends RuntimeException {

    public MongoDbException(final String message) {
        super(message);
    }

    public MongoDbException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * When the flag {@code failOnError} is set in {@link MongoDbSinkWriter}, this exception is
     * raised as soon as any exception occurs when MongoDB is written to.
     */
    static class MongoDbFailFastException extends MongoDbException {

        private static final String ERROR_MESSAGE =
                "Encountered an exception while persisting records, not retrying due to {failOnError} being set.";

        public MongoDbFailFastException() {
            super(ERROR_MESSAGE);
        }

        public MongoDbFailFastException(final Throwable cause) {
            super(ERROR_MESSAGE, cause);
        }
    }
}
