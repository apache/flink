/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.example.record.jdbcinput;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import eu.stratosphere.api.io.jdbc.JDBCInputFormat;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.GenericDataSource;
import eu.stratosphere.api.Job;
import eu.stratosphere.api.Program;
import eu.stratosphere.api.ProgramDescription;
import eu.stratosphere.api.record.io.CsvOutputFormat;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.nephele.client.JobExecutionResult;
import eu.stratosphere.types.PactFloat;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactString;

/**
 * Stand-alone example for the JDBC connector.
 * 
 * NOTE: To run this example, you need the apache derby code in your classpath.
 *       See the Maven file (pom.xml) for a reference to the derby dependency. You can simply
 *       Change the scope of the Maven dependency from test to compile.
 */
public class JDBCInputExample implements Program, ProgramDescription {

	@Override
	public Job createJob(String... args) {
		String url = args.length > 0 ? args[0] : "jdbc:derby:memory:ebookshop";
		String query = args.length > 1 ? args[1] : "select * from books";
		String output = args.length > 2 ? args[2] : "file:///tmp";

		/*
		 * In this example we use the constructor where the url contains all the settings that are needed.
		 * You could also use the default constructor and deliver a Configuration with all the needed settings.
		 * You also could set the settings to the source-instance.
		 */
		GenericDataSource<JDBCInputFormat> source = new GenericDataSource<JDBCInputFormat>(
			new JDBCInputFormat("org.apache.derby.jdbc.EmbeddedDriver", url, query), "Data Source");

		FileDataSink sink = new FileDataSink(new CsvOutputFormat(), output, "Data Output");
		CsvOutputFormat.configureRecordFormat(sink)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactInteger.class, 0)
			.field(PactString.class, 1)
			.field(PactString.class, 2)
			.field(PactFloat.class, 3)
			.field(PactInteger.class, 4);

		sink.addInput(source);
		return new Job(sink, "JDBC Input Example Job");
	}

	@Override
	public String getDescription() {
		return "Parameter: [URL] [Query] [Output File]";
	}

	/*
	 * To run this example, you need the apache derby code in your classpath!
	 */
	public static void main(String[] args) throws Exception {
		
		prepareTestDb();
		
		JDBCInputExample tut = new JDBCInputExample();
		JobExecutionResult res = LocalExecutor.execute(tut, args);
		System.out.println("runtime:  " + res.getNetRuntime());
		
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

		sqlQueryBuilder = new StringBuilder("CREATE TABLE bookscontent (");
		sqlQueryBuilder.append("id INT NOT NULL DEFAULT 0,");
		sqlQueryBuilder.append("title VARCHAR(50) DEFAULT NULL,");
		sqlQueryBuilder.append("content BLOB(10K) DEFAULT NULL,");
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

		sqlQueryBuilder = new StringBuilder("INSERT INTO bookscontent (id, title, content) VALUES ");
		sqlQueryBuilder.append("(1001, 'Java for dummies', CAST(X'7f454c4602' AS BLOB)),");
		sqlQueryBuilder.append("(1002, 'More Java for dummies', CAST(X'7f454c4602' AS BLOB)),");
		sqlQueryBuilder.append("(1003, 'More Java for more dummies', CAST(X'7f454c4602' AS BLOB)),");
		sqlQueryBuilder.append("(1004, 'A Cup of Java', CAST(X'7f454c4602' AS BLOB)),");
		sqlQueryBuilder.append("(1005, 'A Teaspoon of Java', CAST(X'7f454c4602' AS BLOB))");

		stat = conn.createStatement();
		stat.execute(sqlQueryBuilder.toString());
		stat.close();

		conn.close();
	}
}
