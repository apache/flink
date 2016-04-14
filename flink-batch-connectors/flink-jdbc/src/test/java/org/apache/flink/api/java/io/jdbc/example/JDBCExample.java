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
import org.apache.flink.api.java.io.jdbc.JDBCTestBase;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public class JDBCExample {
	
	public static final boolean exploitParallelism = true;
	
	public static void main(String[] args) throws Exception {
		JDBCTestBase.prepareTestDb();

		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		JDBCInputFormatBuilder inputBuilder = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(JDBCTestBase.DRIVER_CLASS)
				.setDBUrl(JDBCTestBase.DB_URL)
				.setQuery(JDBCTestBase.SELECT_ALL_BOOKS);
		
		if(exploitParallelism){
			final String splitColumnName = "id";
			final int fetchSize = 1;
			final Long min = new Long(JDBCTestBase.testData[0][0]+"");
			final Long max = new Long(JDBCTestBase.testData[JDBCTestBase.testData.length-fetchSize][0]+"");
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
				.setDrivername(JDBCTestBase.DRIVER_CLASS)
				.setDBUrl(JDBCTestBase.DB_URL)
				.setQuery("insert into newbooks (id,title,author,price,qty) values (?,?,?,?,?)")
				.finish());
		environment.execute();
	}


}
