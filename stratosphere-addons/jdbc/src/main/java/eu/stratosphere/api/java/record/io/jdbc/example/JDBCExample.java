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
package eu.stratosphere.api.java.record.io.jdbc.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.java.record.operators.GenericDataSink;
import eu.stratosphere.api.java.record.operators.GenericDataSource;
import eu.stratosphere.api.java.record.io.jdbc.JDBCInputFormat;
import eu.stratosphere.api.java.record.io.jdbc.JDBCOutputFormat;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;

/**
 * Stand-alone example for the JDBC connector.
 *
 * NOTE: To run this example, you need the apache derby code in your classpath.
 * See the Maven file (pom.xml) for a reference to the derby dependency. You can
 * simply Change the scope of the Maven dependency from test to compile.
 */
public class JDBCExample implements Program, ProgramDescription {

	@Override
	public Plan getPlan(String[] args) {
		/*
		 * In this example we use the constructor where the url contains all the settings that are needed.
		 * You could also use the default constructor and deliver a Configuration with all the needed settings.
		 * You also could set the settings to the source-instance.
		 */
		GenericDataSource<JDBCInputFormat> source = new GenericDataSource<JDBCInputFormat>(
				new JDBCInputFormat(
						"org.apache.derby.jdbc.EmbeddedDriver",
						"jdbc:derby:memory:ebookshop",
						"select * from books"),
				"Data Source");

		GenericDataSink sink = new GenericDataSink(new JDBCOutputFormat(), "Data Output");
		JDBCOutputFormat.configureOutputFormat(sink)
				.setDriver("org.apache.derby.jdbc.EmbeddedDriver")
				.setUrl("jdbc:derby:memory:ebookshop")
				.setQuery("insert into newbooks (id,title,author,price,qty) values (?,?,?,?,?)")
				.setClass(IntValue.class)
				.setClass(StringValue.class)
				.setClass(StringValue.class)
				.setClass(FloatValue.class)
				.setClass(IntValue.class);

		sink.addInput(source);
		return new Plan(sink, "JDBC Example Job");
	}

	@Override
	public String getDescription() {
		return "Parameter:";
	}

	/*
	 * To run this example, you need the apache derby code in your classpath!
	 */
	public static void main(String[] args) throws Exception {

		prepareTestDb();
		JDBCExample tut = new JDBCExample();
		JobExecutionResult res = LocalExecutor.execute(tut, args);
		System.out.println("runtime: " + res.getNetRuntime());

		System.exit(0);
	}

	private static void prepareTestDb() throws Exception {
		String dbURL = "jdbc:derby:memory:ebookshop;create=true";
		Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
		Connection conn = DriverManager.getConnection(dbURL);

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

		sqlQueryBuilder = new StringBuilder("CREATE TABLE newbooks (");
		sqlQueryBuilder.append("id INT NOT NULL DEFAULT 0,");
		sqlQueryBuilder.append("title VARCHAR(50) DEFAULT NULL,");
		sqlQueryBuilder.append("author VARCHAR(50) DEFAULT NULL,");
		sqlQueryBuilder.append("price FLOAT DEFAULT NULL,");
		sqlQueryBuilder.append("qty INT DEFAULT NULL,");
		sqlQueryBuilder.append("PRIMARY KEY (id))");

		stat = conn.createStatement();
		stat.executeUpdate(sqlQueryBuilder.toString());
		stat.close();

		sqlQueryBuilder = new StringBuilder("INSERT INTO books (id, title, author, price, qty) VALUES ");
		sqlQueryBuilder.append("(1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11),");
		sqlQueryBuilder.append("(1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22),");
		sqlQueryBuilder.append("(1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33),");
		sqlQueryBuilder.append("(1004, 'A Cup of Java', 'Kumar', 44.44, 44),");
		sqlQueryBuilder.append("(1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)");

		stat = conn.createStatement();
		stat.execute(sqlQueryBuilder.toString());
		stat.close();
		
		conn.close();
	}
}
