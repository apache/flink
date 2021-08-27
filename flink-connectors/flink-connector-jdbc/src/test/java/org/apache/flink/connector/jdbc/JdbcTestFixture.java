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

package org.apache.flink.connector.jdbc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.xa.h2.H2DbMetadata;
import org.apache.flink.table.types.logical.RowType;

import java.io.OutputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/** Test data and helper objects for JDBC tests. */
@SuppressWarnings("SpellCheckingInspection")
public class JdbcTestFixture {
    public static final JdbcTestCheckpoint CP0 = new JdbcTestCheckpoint(0, 1, 2, 3);
    public static final JdbcTestCheckpoint CP1 = new JdbcTestCheckpoint(1, 4, 5, 6);

    public static final String INPUT_TABLE = "books";
    public static final String OUTPUT_TABLE = "newbooks";
    public static final String OUTPUT_TABLE_2 = "newbooks2";
    public static final String OUTPUT_TABLE_3 = "newbooks3";
    public static final String WORDS_TABLE = "words";
    public static final String SELECT_ALL_BOOKS = "select * from " + INPUT_TABLE;
    public static final String SELECT_ID_BOOKS = "select id from " + INPUT_TABLE;
    public static final String SELECT_ALL_NEWBOOKS = "select * from " + OUTPUT_TABLE;
    public static final String SELECT_ALL_NEWBOOKS_2 = "select * from " + OUTPUT_TABLE_2;
    public static final String SELECT_ALL_NEWBOOKS_3 = "select * from " + OUTPUT_TABLE_3;
    public static final String SELECT_EMPTY = "select * from books WHERE QTY < 0";
    public static final String INSERT_TEMPLATE =
            "insert into %s (id, title, author, price, qty) values (?,?,?,?,?)";
    public static final String INSERT_INTO_WORDS_TEMPLATE =
            "insert into words (id, word) values(?, ?)";

    public static final String SELECT_ALL_BOOKS_SPLIT_BY_ID =
            SELECT_ALL_BOOKS + " WHERE id BETWEEN ? AND ?";
    public static final String SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR =
            SELECT_ALL_BOOKS + " WHERE author = ?";

    public static final TestEntry[] TEST_DATA = {
        new TestEntry(1001, ("Java public for dummies"), ("Tan Ah Teck"), 11.11, 11),
        new TestEntry(1002, ("More Java for dummies"), ("Tan Ah Teck"), 22.22, 22),
        new TestEntry(1003, ("More Java for more dummies"), ("Mohammad Ali"), 33.33, 33),
        new TestEntry(1004, ("A Cup of Java"), ("Kumar"), 44.44, 44),
        new TestEntry(1005, ("A Teaspoon of Java"), ("Kevin Jones"), 55.55, 55),
        new TestEntry(1006, ("A Teaspoon of Java 1.4"), ("Kevin Jones"), 66.66, 66),
        new TestEntry(1007, ("A Teaspoon of Java 1.5"), ("Kevin Jones"), 77.77, 77),
        new TestEntry(1008, ("A Teaspoon of Java 1.6"), ("Kevin Jones"), 88.88, 88),
        new TestEntry(1009, ("A Teaspoon of Java 1.7"), ("Kevin Jones"), 99.99, 99),
        new TestEntry(1010, ("A Teaspoon of Java 1.8"), ("Kevin Jones"), null, 1010)
    };

    private static final String EBOOKSHOP_SCHEMA_NAME = "ebookshop";
    public static final DerbyDbMetadata DERBY_EBOOKSHOP_DB =
            new DerbyDbMetadata(EBOOKSHOP_SCHEMA_NAME);
    public static final H2DbMetadata H2_EBOOKSHOP_DB = new H2DbMetadata(EBOOKSHOP_SCHEMA_NAME);

    /** TestEntry. */
    public static class TestEntry implements Serializable {
        public final Integer id;
        public final String title;
        public final String author;
        public final Double price;
        public final Integer qty;

        public TestEntry(Integer id, String title, String author, Double price, Integer qty) {
            this.id = id;
            this.title = title;
            this.author = author;
            this.price = price;
            this.qty = qty;
        }

        @Override
        public String toString() {
            return "TestEntry{"
                    + "id="
                    + id
                    + ", title='"
                    + title
                    + '\''
                    + ", author='"
                    + author
                    + '\''
                    + ", price="
                    + price
                    + ", qty="
                    + qty
                    + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestEntry)) {
                return false;
            }
            TestEntry testEntry = (TestEntry) o;
            return Objects.equals(id, testEntry.id)
                    && Objects.equals(title, testEntry.title)
                    && Objects.equals(author, testEntry.author)
                    && Objects.equals(price, testEntry.price)
                    && Objects.equals(qty, testEntry.qty);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, title, author, price, qty);
        }
    }

    public static final RowTypeInfo ROW_TYPE_INFO =
            new RowTypeInfo(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    public static final RowType ROW_TYPE =
            (RowType) fromLegacyInfoToDataType(ROW_TYPE_INFO).getLogicalType();

    private static String getCreateQuery(String tableName) {
        return "CREATE TABLE "
                + tableName
                + " ("
                + "id INT NOT NULL DEFAULT 0,"
                + "title VARCHAR(50) DEFAULT NULL,"
                + "author VARCHAR(50) DEFAULT NULL,"
                + "price FLOAT DEFAULT NULL,"
                + "qty INT DEFAULT NULL,"
                + "PRIMARY KEY (id))";
    }

    public static String getInsertQuery() {
        StringBuilder sqlQueryBuilder =
                new StringBuilder("INSERT INTO books (id, title, author, price, qty) VALUES ");
        for (int i = 0; i < TEST_DATA.length; i++) {
            sqlQueryBuilder
                    .append("(")
                    .append(TEST_DATA[i].id)
                    .append(",'")
                    .append(TEST_DATA[i].title)
                    .append("','")
                    .append(TEST_DATA[i].author)
                    .append("',")
                    .append(TEST_DATA[i].price)
                    .append(",")
                    .append(TEST_DATA[i].qty)
                    .append(")");
            if (i < TEST_DATA.length - 1) {
                sqlQueryBuilder.append(",");
            }
        }
        return sqlQueryBuilder.toString();
    }

    @SuppressWarnings("unused") // used in string constant in prepareDatabase
    public static final OutputStream DEV_NULL =
            new OutputStream() {
                @Override
                public void write(int b) {}
            };

    public static void initSchema(DbMetadata dbMetadata)
            throws ClassNotFoundException, SQLException {
        System.setProperty(
                "derby.stream.error.field", JdbcTestFixture.class.getCanonicalName() + ".DEV_NULL");
        Class.forName(dbMetadata.getDriverClass());
        try (Connection conn =
                DriverManager.getConnection(
                        dbMetadata.getInitUrl(), dbMetadata.getUser(), dbMetadata.getPassword())) {
            createTable(conn, JdbcTestFixture.INPUT_TABLE);
            createTable(conn, OUTPUT_TABLE);
            createTable(conn, OUTPUT_TABLE_2);
            createTable(conn, OUTPUT_TABLE_3);
            createWordsTable(conn);
        }
    }

    private static void createWordsTable(Connection conn) throws SQLException {
        executeUpdate(
                conn,
                "create table "
                        + WORDS_TABLE
                        + " (id int not null, word varchar(50), primary key (id))");
    }

    private static void executeUpdate(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate(sql);
        }
    }

    static void initData(DbMetadata dbMetadata) throws SQLException {
        try (Connection conn = DriverManager.getConnection(dbMetadata.getUrl())) {
            insertDataIntoInputTable(conn);
        }
    }

    private static void createTable(Connection conn, String tableName) throws SQLException {
        executeUpdate(conn, getCreateQuery(tableName));
    }

    private static void insertDataIntoInputTable(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute(getInsertQuery());
        stat.close();
    }

    public static void cleanUpDatabasesStatic(DbMetadata dbMetadata)
            throws ClassNotFoundException, SQLException {
        Class.forName(dbMetadata.getDriverClass());
        try (Connection conn = DriverManager.getConnection(dbMetadata.getUrl());
                Statement stat = conn.createStatement()) {

            stat.executeUpdate("DROP TABLE " + INPUT_TABLE);
            stat.executeUpdate("DROP TABLE " + OUTPUT_TABLE);
            stat.executeUpdate("DROP TABLE " + OUTPUT_TABLE_2);
            stat.executeUpdate("DROP TABLE " + OUTPUT_TABLE_3);
            stat.executeUpdate("DROP TABLE " + WORDS_TABLE);
        }
    }

    static void cleanupData(String url) throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            executeUpdate(conn, "delete from " + INPUT_TABLE);
            executeUpdate(conn, "delete from " + WORDS_TABLE);
        }
    }
}
