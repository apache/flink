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

package org.apache.flink.connector.jdbc.internal.options;

import org.apache.flink.util.Preconditions;

import java.util.stream.IntStream;

/** JDBC sink insert options. */
public class JdbcInsertOptions extends JdbcTypedQueryOptions {

    private static final long serialVersionUID = 1L;

    private final String query;

    public JdbcInsertOptions(String query, int[] typesArray) {
        super(typesArray);
        this.query = Preconditions.checkNotNull(query, "query is empty");
    }

    public String getQuery() {
        return query;
    }

    public static JdbcInsertOptions from(String query, int firstFieldType, int... nextFieldTypes) {
        return new JdbcInsertOptions(query, concat(firstFieldType, nextFieldTypes));
    }

    private static int[] concat(int first, int... next) {
        if (next == null || next.length == 0) {
            return new int[] {first};
        } else {
            return IntStream.concat(IntStream.of(new int[] {first}), IntStream.of(next)).toArray();
        }
    }
}
