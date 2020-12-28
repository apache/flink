/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

/** {@link Matcher Matchers} for asserting {@link Operation}. */
public class OperationMatchers {
    /**
     * Checks for a {@link CreateTableOperation}. You can provide additional matchers as parameters.
     * All of the provided nested matchers must match.
     *
     * <p>Example:
     *
     * <pre>{@code
     * assertThat(
     * 	operation,
     * 	createTableOperation(
     * 		hasSchema(
     * 			TableSchema.builder()
     * 				.field("f0", DataTypes.INT().notNull())
     * 				.field("f1", DataTypes.TIMESTAMP(3))
     * 				.field("a", DataTypes.INT())
     * 				.watermark("f1", "`f1` - INTERVAL '5' SECOND", DataTypes.TIMESTAMP(3))
     * 				.build()
     * 		),
     * 		hasOptions(
     * 			entry("connector.type", "kafka"),
     * 			entry("format.type", "json")
     * 		),
     * 		hasPartition(
     * 			"a", "f0"
     * 		)
     * 	)
     * );
     * }</pre>
     *
     * @param nestedMatchers additional matchers that must match
     * @see #withOptions(MapEntry[])
     * @see #withSchema(TableSchema)
     * @see #partitionedBy(String...)
     */
    @SafeVarargs
    public static Matcher<Operation> isCreateTableOperation(
            Matcher<CreateTableOperation>... nestedMatchers) {
        return new CreateTableOperationMatcher(nestedMatchers);
    }

    /**
     * Checks that the {@link CreateTableOperation} has all given connector options.
     *
     * @param entries Connector options to check.
     * @see #entry(String, String)
     */
    @SafeVarargs
    public static Matcher<CreateTableOperation> withOptions(MapEntry<String, String>... entries) {
        return new FeatureMatcher<CreateTableOperation, Map<String, String>>(
                equalTo(mapOf(entries)), "options of the derived table", "options") {
            @Override
            protected Map<String, String> featureValueOf(CreateTableOperation actual) {
                return actual.getCatalogTable().getProperties();
            }
        };
    }

    /** Helper method for easier creating maps in matchers. */
    public static MapEntry<String, String> entry(String key, String value) {
        return new MapEntry<>(key, value);
    }

    /**
     * Checks that the {@link CreateTableOperation} is partitioned by given fields.
     *
     * @param fields Fields on which the Table should be partitioned.
     * @see #isCreateTableOperation(Matcher[])
     */
    public static Matcher<CreateTableOperation> partitionedBy(String... fields) {
        return new FeatureMatcher<CreateTableOperation, List<String>>(
                equalTo(Arrays.asList(fields)), "partitions of the derived table", "partitions") {
            @Override
            protected List<String> featureValueOf(CreateTableOperation actual) {
                return actual.getCatalogTable().getPartitionKeys();
            }
        };
    }

    /**
     * Checks that the schema of {@link CreateTableOperation} is equal to the given {@link
     * TableSchema}.
     *
     * @param schema TableSchema that the {@link CreateTableOperation} should have
     * @see #isCreateTableOperation(Matcher[])
     */
    public static Matcher<CreateTableOperation> withSchema(TableSchema schema) {
        return new FeatureMatcher<CreateTableOperation, TableSchema>(
                equalTo(schema), "table schema of the derived table", "table schema") {
            @Override
            protected TableSchema featureValueOf(CreateTableOperation actual) {
                return actual.getCatalogTable().getSchema();
            }
        };
    }

    /** Helper class for easier creating maps in matchers. */
    public static class MapEntry<K, V> {
        private final K key;
        private final V value;

        public MapEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    @SafeVarargs
    private static Map<String, String> mapOf(MapEntry<String, String>... entries) {
        Map<String, String> map = new HashMap<>();
        for (MapEntry<String, String> entry : entries) {
            map.put(entry.key, entry.value);
        }

        return map;
    }

    private static class CreateTableOperationMatcher extends TypeSafeDiagnosingMatcher<Operation> {
        private final Matcher<CreateTableOperation>[] nestedMatchers;

        public CreateTableOperationMatcher(Matcher<CreateTableOperation>[] nestedMatchers) {
            this.nestedMatchers = nestedMatchers;
        }

        @Override
        protected boolean matchesSafely(Operation item, Description mismatchDescription) {
            if (!(item instanceof CreateTableOperation)) {

                return false;
            }

            for (Matcher<CreateTableOperation> nestedMatcher : nestedMatchers) {
                if (!nestedMatcher.matches(item)) {
                    nestedMatcher.describeMismatch(item, mismatchDescription);
                    return false;
                }
            }

            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("\n");
            Arrays.stream(nestedMatchers)
                    .forEach(
                            matcher -> {
                                matcher.describeTo(description);
                                description.appendText("\n");
                            });
        }
    }
}
