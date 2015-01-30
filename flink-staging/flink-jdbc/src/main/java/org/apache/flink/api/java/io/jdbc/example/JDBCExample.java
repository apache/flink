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

package org.apache.flink.api.java.io.jdbc.example;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public class JDBCExample {

	public static void main(String[] args) throws Exception {
		prepareTestDb();

		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5> source
				= environment.createInput(JDBCInputFormat.buildJDBCInputFormat()
						.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
						.setDBUrl("jdbc:derby:memory:ebookshop")
						.setQuery("select * from books")
						.finish(),
						new TupleTypeInfo(Tuple5.class, INT_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO, DOUBLE_TYPE_INFO, INT_TYPE_INFO)
				);

		source.output(JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
				.setDBUrl("jdbc:derby:memory:ebookshop")
				.setQuery("insert into newbooks (id,title,author,price,qty) values (?,?,?,?,?)")
				.finish());
		environment.execute();
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
