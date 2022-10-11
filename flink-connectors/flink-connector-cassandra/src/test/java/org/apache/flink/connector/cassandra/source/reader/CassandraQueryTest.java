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

package org.apache.flink.connector.cassandra.source.reader;

import org.apache.flink.connector.cassandra.source.CassandraSource;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** tests for query generation and query sanity checks. */
class CassandraQueryTest {

    @Test
    public void testKeySpaceTableExtractionRegexp() {
        final Pattern pattern = Pattern.compile(CassandraSplitReader.SELECT_REGEXP);
        Matcher matcher;
        matcher = pattern.matcher("SELECT field FROM keyspace.table where field = value;");
        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.group(1)).isEqualTo("keyspace");
        assertThat(matcher.group(2)).isEqualTo("table");

        matcher = pattern.matcher("SELECT * FROM keyspace.table;");
        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.group(1)).isEqualTo("keyspace");
        assertThat(matcher.group(2)).isEqualTo("table");

        matcher = pattern.matcher("select field1, field2 from keyspace.table;");
        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.group(1)).isEqualTo("keyspace");
        assertThat(matcher.group(2)).isEqualTo("table");

        matcher = pattern.matcher("select field1, field2 from keyspace.table LIMIT(1000);");
        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.group(1)).isEqualTo("keyspace");
        assertThat(matcher.group(2)).isEqualTo("table");

        matcher = pattern.matcher("select field1 from keyspace.table ;");
        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.group(1)).isEqualTo("keyspace");
        assertThat(matcher.group(2)).isEqualTo("table");

        matcher = pattern.matcher("select field1 from keyspace.table where field1=1;");
        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.group(1)).isEqualTo("keyspace");
        assertThat(matcher.group(2)).isEqualTo("table");

        matcher = pattern.matcher("select field1 from table;"); // missing keyspace
        assertThat(matcher.matches()).isFalse();

        matcher = pattern.matcher("select field1 from keyspace.table"); // missing ";"
        assertThat(matcher.matches()).isFalse();
    }

    @Test
    public void testProhibitedClauses() {
        assertThatThrownBy(
                        () ->
                                CassandraSource.checkQueryValidity(
                                        "SELECT COUNT(*) from flink.table;"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("query must not contain aggregate or order clauses ");
        assertThatThrownBy(
                        () -> CassandraSource.checkQueryValidity("SELECT AVG(*) from flink.table;"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("query must not contain aggregate or order clauses ");

        assertThatThrownBy(
                        () -> CassandraSource.checkQueryValidity("SELECT MIN(*) from flink.table;"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("query must not contain aggregate or order clauses ");
        assertThatThrownBy(
                        () -> CassandraSource.checkQueryValidity("SELECT MAX(*) from flink.table;"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("query must not contain aggregate or order clauses ");
        assertThatThrownBy(
                        () -> CassandraSource.checkQueryValidity("SELECT SUM(*) from flink.table;"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("query must not contain aggregate or order clauses ");
        assertThatThrownBy(
                        () ->
                                CassandraSource.checkQueryValidity(
                                        "SELECT field1, field2 from flink.table ORDER BY field1;"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("query must not contain aggregate or order clauses ");
        assertThatThrownBy(
                        () ->
                                CassandraSource.checkQueryValidity(
                                        "SELECT field1, field2 from flink.table GROUP BY field1;"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("query must not contain aggregate or order clauses ");
    }

    @Test
    public void testGenerateRangeQuery() {
        String query;
        String outputQuery;

        // query with where clause
        query = "SELECT field FROM keyspace.table WHERE field = value;";
        outputQuery = CassandraSplitReader.generateRangeQuery(query, "field");
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT field FROM keyspace.table WHERE (token(field) >= ?) AND (token(field) < ?) AND field = value;");

        // query without where clause
        query = "SELECT * FROM keyspace.table;";
        outputQuery = CassandraSplitReader.generateRangeQuery(query, "field");
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT * FROM keyspace.table WHERE (token(field) >= ?) AND (token(field) < ?);");

        // query without where clause but with another trailing clause
        query = "SELECT field FROM keyspace.table LIMIT(1000);";
        outputQuery = CassandraSplitReader.generateRangeQuery(query, "field");
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT field FROM keyspace.table WHERE (token(field) >= ?) AND (token(field) < ?) LIMIT(1000);");
    }

    @Test
    public void testGetHighestSplitQuery() {
        String query;
        String outputQuery;

        // query with where clause
        query = "SELECT field FROM keyspace.table WHERE field = value;";
        outputQuery = CassandraSplitReader.getHighestSplitQuery(query, "field", BigInteger.ZERO);
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT field FROM keyspace.table WHERE (token(field) >= 0) AND field = value;");

        // query without where clause
        query = "SELECT * FROM keyspace.table;";
        outputQuery = CassandraSplitReader.getHighestSplitQuery(query, "field", BigInteger.ZERO);
        assertThat(outputQuery)
                .isEqualTo("SELECT * FROM keyspace.table WHERE (token(field) >= 0);");

        // query without where clause but with another trailing clause
        query = "SELECT field FROM keyspace.table LIMIT(1000);";
        outputQuery = CassandraSplitReader.getHighestSplitQuery(query, "field", BigInteger.ZERO);
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT field FROM keyspace.table WHERE (token(field) >= 0) LIMIT(1000);");
    }

    @Test
    public void testGetLowestSplitQuery() {
        String query;
        String outputQuery;

        // query with where clause
        query = "SELECT field FROM keyspace.table WHERE field = value;";
        outputQuery = CassandraSplitReader.getLowestSplitQuery(query, "field", BigInteger.ZERO);
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT field FROM keyspace.table WHERE (token(field) < 0) AND field = value;");

        // query without where clause
        query = "SELECT * FROM keyspace.table;";
        outputQuery = CassandraSplitReader.getLowestSplitQuery(query, "field", BigInteger.ZERO);
        assertThat(outputQuery).isEqualTo("SELECT * FROM keyspace.table WHERE (token(field) < 0);");

        // query without where clause but with another trailing clause
        query = "SELECT field FROM keyspace.table LIMIT(1000);";
        outputQuery = CassandraSplitReader.getLowestSplitQuery(query, "field", BigInteger.ZERO);
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT field FROM keyspace.table WHERE (token(field) < 0) LIMIT(1000);");
    }
}
