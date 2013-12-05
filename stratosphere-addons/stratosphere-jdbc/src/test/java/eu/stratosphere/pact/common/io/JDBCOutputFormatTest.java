/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.pact.common.io;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactFloat;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
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

public class JDBCOutputFormatTest {
    private JDBCInputFormat jdbcInputFormat;
    private JDBCOutputFormat jdbcOutputFormat;

    private static Connection conn;

    static final Value[][] dbData = {
        {new PactInteger(1001), new PactString("Java for dummies"), new PactString("Tan Ah Teck"), new PactDouble(11.11), new PactInteger(11)},
        {new PactInteger(1002), new PactString("More Java for dummies"), new PactString("Tan Ah Teck"), new PactDouble(22.22), new PactInteger(22)},
        {new PactInteger(1003), new PactString("More Java for more dummies"), new PactString("Mohammad Ali"), new PactDouble(33.33), new PactInteger(33)},
        {new PactInteger(1004), new PactString("A Cup of Java"), new PactString("Kumar"), new PactDouble(44.44), new PactInteger(44)},
        {new PactInteger(1005), new PactString("A Teaspoon of Java"), new PactString("Kevin Jones"), new PactDouble(55.55), new PactInteger(55)}};

    @BeforeClass
    public static void setUpClass() {
        try {
            prepareDerbyInputDatabase();
            prepareDerbyOutputDatabase();
        } catch (ClassNotFoundException e) {
            Assert.fail();
        }
    }

    private static void prepareDerbyInputDatabase() throws ClassNotFoundException {
        try {
            String dbURL = "jdbc:derby:memory:ebookshop;create=true";
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            conn = DriverManager.getConnection(dbURL);
            createTableBooks();
            insertDataToSQLTables();
            conn.close();
        } catch (ClassNotFoundException e) {
            Assert.fail();
        } catch (SQLException e) {
            Assert.fail();
        }
    }

    private static void prepareDerbyOutputDatabase() throws ClassNotFoundException {
        try {
            String dbURL = "jdbc:derby:memory:ebookshop;create=true";
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            conn = DriverManager.getConnection(dbURL);
            createTableNewBooks();
            conn.close();
        } catch (ClassNotFoundException e) {
            Assert.fail();
        } catch (SQLException e) {
            Assert.fail();
        }
    }

    private static void createTableBooks() throws SQLException {
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
    }

    private static void createTableNewBooks() throws SQLException {
        StringBuilder sqlQueryBuilder = new StringBuilder("CREATE TABLE newbooks (");
        sqlQueryBuilder.append("id INT NOT NULL DEFAULT 0,");
        sqlQueryBuilder.append("title VARCHAR(50) DEFAULT NULL,");
        sqlQueryBuilder.append("author VARCHAR(50) DEFAULT NULL,");
        sqlQueryBuilder.append("price FLOAT DEFAULT NULL,");
        sqlQueryBuilder.append("qty INT DEFAULT NULL,");
        sqlQueryBuilder.append("PRIMARY KEY (id))");

        Statement stat = conn.createStatement();
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
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
        jdbcOutputFormat = null;
    }

    @Test
    public void testJDBCOutputFormat() throws IOException {
        String sourceTable = "books";
        String targetTable = "newbooks";
        String driverPath = "org.apache.derby.jdbc.EmbeddedDriver";
        String dbUrl = "jdbc:derby:memory:ebookshop";
        
        Configuration cfg = new Configuration();
        cfg.setString("driver", driverPath);
        cfg.setString("url", dbUrl);
        cfg.setString("query", "insert into " + targetTable + " (id, title, author, price, qty) values (?,?,?,?,?)");
        cfg.setInteger("fields", 5);
        cfg.setClass("type0", PactInteger.class);
        cfg.setClass("type1", PactString.class);
        cfg.setClass("type2", PactString.class);
        cfg.setClass("type3", PactFloat.class);
        cfg.setClass("type4", PactInteger.class);

        jdbcOutputFormat = new JDBCOutputFormat();
        jdbcOutputFormat.configure(cfg);
        jdbcOutputFormat.open(1);

        jdbcInputFormat = new JDBCInputFormat(
                driverPath,
                dbUrl,
                "select * from " + sourceTable);
        jdbcInputFormat.configure(null);

        PactRecord record = new PactRecord();
        while (!jdbcInputFormat.reachedEnd()) {
            jdbcInputFormat.nextRecord(record);
            jdbcOutputFormat.writeRecord(record);
        }

        jdbcOutputFormat.close();
        jdbcInputFormat.close();

        jdbcInputFormat = new JDBCInputFormat(
                driverPath,
                dbUrl,
                "select * from " + targetTable);
        jdbcInputFormat.configure(null);

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

        jdbcInputFormat.close();
    }
}
