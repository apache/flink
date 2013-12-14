/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
* http://www.apache.org/licenses/LICENSE-2.0
 * 
* Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package eu.stratosphere.pact.common.io;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.PactDouble;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.PactString;
import eu.stratosphere.types.Value;

public class JDBCInputFormatTest {
    JDBCInputFormat jdbcInputFormat;
    Configuration config;
    static Connection conn;
    static final Value[][] dbData = {
        {new PactInteger(1001), new PactString("Java for dummies"), new PactString("Tan Ah Teck"), new PactDouble(11.11), new PactInteger(11)},
        {new PactInteger(1002), new PactString("More Java for dummies"), new PactString("Tan Ah Teck"), new PactDouble(22.22), new PactInteger(22)},
        {new PactInteger(1003), new PactString("More Java for more dummies"), new PactString("Mohammad Ali"), new PactDouble(33.33), new PactInteger(33)},
        {new PactInteger(1004), new PactString("A Cup of Java"), new PactString("Kumar"), new PactDouble(44.44), new PactInteger(44)},
        {new PactInteger(1005), new PactString("A Teaspoon of Java"), new PactString("Kevin Jones"), new PactDouble(55.55), new PactInteger(55)}};

    @BeforeClass
    public static void setUpClass() {
        try {
            prepareDerbyDatabase();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private static void prepareDerbyDatabase() throws ClassNotFoundException {
        String dbURL = "jdbc:derby:memory:ebookshop;create=true";
        createConnection(dbURL);
    }

    /*
     Loads JDBC derby driver ; creates(if necessary) and populates database.
     */
    private static void createConnection(String dbURL) {
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            conn = DriverManager.getConnection(dbURL);
            createTable();
            insertDataToSQLTables();
            conn.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            Assert.fail();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private static void createTable() throws SQLException {
        StringBuilder sqlQueryBuilder = new StringBuilder("CREATE TABLE books (");
        sqlQueryBuilder.append("id INT NOT NULL DEFAULT 0,");
        sqlQueryBuilder.append("title VARCHAR(50) DEFAULT NULL,");
        sqlQueryBuilder.append("author VARCHAR(50) DEFAULT NULL,");
        sqlQueryBuilder.append("price FLOAT DEFAULT NULL,");
        sqlQueryBuilder.append("qty INT DEFAULT NULL,");
        sqlQueryBuilder.append("PRIMARY KEY (id))");

        Statement stat = conn.createStatement();
        stat.executeUpdate(sqlQueryBuilder.toString());
        stat.close();

        sqlQueryBuilder = new StringBuilder("CREATE TABLE bookscontent (");
        sqlQueryBuilder.append("id INT NOT NULL DEFAULT 0,");
        sqlQueryBuilder.append("title VARCHAR(50) DEFAULT NULL,");
        sqlQueryBuilder.append("content BLOB(10K) DEFAULT NULL,");
        sqlQueryBuilder.append("PRIMARY KEY (id))");

        stat = conn.createStatement();
        stat.executeUpdate(sqlQueryBuilder.toString());
        stat.close();
    }

    private static void insertDataToSQLTables() throws SQLException {
        StringBuilder sqlQueryBuilder = new StringBuilder("INSERT INTO books (id, title, author, price, qty) VALUES ");
        sqlQueryBuilder.append("(1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11),");
        sqlQueryBuilder.append("(1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22),");
        sqlQueryBuilder.append("(1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33),");
        sqlQueryBuilder.append("(1004, 'A Cup of Java', 'Kumar', 44.44, 44),");
        sqlQueryBuilder.append("(1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)");

        Statement stat = conn.createStatement();
        stat.execute(sqlQueryBuilder.toString());
        stat.close();

        sqlQueryBuilder = new StringBuilder("INSERT INTO bookscontent (id, title, content) VALUES ");
        sqlQueryBuilder.append("(1001, 'Java for dummies', CAST(X'7f454c4602' AS BLOB)),");
        sqlQueryBuilder.append("(1002, 'More Java for dummies', CAST(X'7f454c4602' AS BLOB)),");
        sqlQueryBuilder.append("(1003, 'More Java for more dummies', CAST(X'7f454c4602' AS BLOB)),");
        sqlQueryBuilder.append("(1004, 'A Cup of Java', CAST(X'7f454c4602' AS BLOB)),");
        sqlQueryBuilder.append("(1005, 'A Teaspoon of Java', CAST(X'7f454c4602' AS BLOB))");

        stat = conn.createStatement();
        stat.execute(sqlQueryBuilder.toString());
        stat.close();
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
        jdbcInputFormat = null;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidConnection() {
        jdbcInputFormat = new JDBCInputFormat("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:idontexist", "select * from books;");
        jdbcInputFormat.configure(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidQuery() {
        jdbcInputFormat = new JDBCInputFormat("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:ebookshop", "abc");
        jdbcInputFormat.configure(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDBType() {
        jdbcInputFormat = new JDBCInputFormat("idontexist.Driver", "jdbc:derby:memory:ebookshop", "select * from books;");
        jdbcInputFormat.configure(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedSQLType() {
        jdbcInputFormat = new JDBCInputFormat("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:ebookshop", "select * from bookscontent");
        jdbcInputFormat.configure(null);
        jdbcInputFormat.nextRecord(new PactRecord());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNotConfiguredFormatNext() {
        jdbcInputFormat = new JDBCInputFormat("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:ebookshop", "select * from books");
        jdbcInputFormat.nextRecord(new PactRecord());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNotConfiguredFormatEnd() {
        jdbcInputFormat = new JDBCInputFormat("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:ebookshop", "select * from books");
        jdbcInputFormat.reachedEnd();
    }

    @Test
    public void testJDBCInputFormat() throws IOException {
        jdbcInputFormat = new JDBCInputFormat("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:ebookshop", "select * from books");
        jdbcInputFormat.configure(null);
        PactRecord record = new PactRecord();
        int recordCount = 0;
        while (!jdbcInputFormat.reachedEnd()) {
            jdbcInputFormat.nextRecord(record);
            Assert.assertEquals(5, record.getNumFields());
            Assert.assertEquals("Field 0 should be int", PactInteger.class, record.getField(0, PactInteger.class).getClass());
            Assert.assertEquals("Field 1 should be String", PactString.class, record.getField(1, PactString.class).getClass());
            Assert.assertEquals("Field 2 should be String", PactString.class, record.getField(2, PactString.class).getClass());
            Assert.assertEquals("Field 3 should be float", PactDouble.class, record.getField(3, PactDouble.class).getClass());
            Assert.assertEquals("Field 4 should be int", PactInteger.class, record.getField(4, PactInteger.class).getClass());

            int[] pos = {0, 1, 2, 3, 4};
            Value[] values = {new PactInteger(), new PactString(), new PactString(), new PactDouble(), new PactInteger()};
            Assert.assertTrue(record.equalsFields(pos, dbData[recordCount], values));

            recordCount++;
        }
        Assert.assertEquals(5, recordCount);
    }

}
