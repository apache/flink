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
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public class JDBCExample {
	
	public static final String DRIVER_CLASS = "org.apache.derby.jdbc.EmbeddedDriver";
	public static final String DB_URL = "jdbc:derby:memory:ebookshop";
	public static final String INPUT_TABLE = "books";
	public static final String SELECT_ALL_BOOKS = "select * from books";

	public static final boolean exploitParallelism = true;

	public static final Object[][] testData = {
			{1001, ("Java public for dummies"), ("Tan Ah Teck"), 11.11, 11},
			{1002, ("More Java for dummies"), ("Tan Ah Teck"), 22.22, 22},
			{1003, ("More Java for more dummies"), ("Mohammad Ali"), 33.33, 33},
			{1004, ("A Cup of Java"), ("Kumar"), 44.44, 44},
			{1005, ("A Teaspoon of Java"), ("Kevin Jones"), 55.55, 55},
			{1006, ("A Teaspoon of Java 1.4"), ("Kevin Jones"), 66.66, 66},
			{1007, ("A Teaspoon of Java 1.5"), ("Kevin Jones"), 77.77, 77},
			{1008, ("A Teaspoon of Java 1.6"), ("Kevin Jones"), 88.88, 88},
			{1009, ("A Teaspoon of Java 1.7"), ("Kevin Jones"), 99.99, 99},
			{1010, ("A Teaspoon of Java 1.8"), ("Kevin Jones"), null, 1010}};

	
	public static void main(String[] args) throws Exception {
		prepareTestDb();

		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		JDBCInputFormatBuilder inputBuilder = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(SELECT_ALL_BOOKS);
		
		if(exploitParallelism){
			final String splitColumnName = "id";
			final int fetchSize = 1;
			final Long min = new Long(testData[0][0]+"");
			final Long max = new Long(testData[testData.length-fetchSize][0]+"");
			//rewrite query and add $CONDITIONS token to generate splits (sqoop-like)
			inputBuilder = inputBuilder
					// WARNING: ONLY when query does not contains the  WHERE clause we can keep the next line commented
					//.setQuery(SELECT_ALL_BOOKS + " WHERE " +JDBCInputFormat.CONDITIONS)
					.setSplitConfig(splitColumnName, fetchSize, min, max);
		}
		DataSet<Tuple5> source
				= environment.createInput(
						inputBuilder.finish(),
						new TupleTypeInfo(Tuple5.class, INT_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO, DOUBLE_TYPE_INFO, INT_TYPE_INFO)
				);

		source.output(JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery("insert into newbooks (id,title,author,price,qty) values (?,?,?,?,?)")
				.finish());
		environment.execute();
	}

	private static void prepareTestDb() throws Exception {
		System.setProperty("derby.stream.error.field", "org.apache.flink.api.java.io.jdbc.DerbyUtil.DEV_NULL");
		Class.forName(DRIVER_CLASS);
		Connection conn = DriverManager.getConnection(DB_URL+";create=true");

		//create output table
		Statement stat = conn.createStatement();
		stat.executeUpdate(getCreateQuery().replace("books", "newbooks"));
		stat.close();

		//create input table
		stat = conn.createStatement();
		stat.executeUpdate(getCreateQuery());
		stat.close();

		//prepare input data
		stat = conn.createStatement();
		stat.execute(getInsertQuery());
		stat.close();

		conn.close();
	}
	

	public static String getCreateQuery() {
		StringBuilder sqlQueryBuilder = new StringBuilder("CREATE TABLE books (");
		sqlQueryBuilder.append("id INT NOT NULL DEFAULT 0,");
		sqlQueryBuilder.append("title VARCHAR(50) DEFAULT NULL,");
		sqlQueryBuilder.append("author VARCHAR(50) DEFAULT NULL,");
		sqlQueryBuilder.append("price FLOAT DEFAULT NULL,");
		sqlQueryBuilder.append("qty INT DEFAULT NULL,");
		sqlQueryBuilder.append("PRIMARY KEY (id))");
		return sqlQueryBuilder.toString();
	}
	
	public static String getInsertQuery() {
		StringBuilder sqlQueryBuilder = new StringBuilder("INSERT INTO books (id, title, author, price, qty) VALUES ");
		for (int i = 0; i < testData.length; i++) {
			sqlQueryBuilder.append("(")
			.append(testData[i][0]).append(",'")
			.append(testData[i][1]).append("','")
			.append(testData[i][2]).append("',")
			.append(testData[i][3]).append(",")
			.append(testData[i][4]).append(")");
			if(i<testData.length-1)
				sqlQueryBuilder.append(",");
		}
		String insertQuery = sqlQueryBuilder.toString();
		return insertQuery;
	}
}
