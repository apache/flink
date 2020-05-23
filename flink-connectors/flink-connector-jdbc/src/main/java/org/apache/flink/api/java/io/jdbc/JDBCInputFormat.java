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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.io.jdbc.split.ParameterValuesProvider;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * InputFormat to read data from a database and generate Rows.
 * The InputFormat has to be configured using the supplied InputFormatBuilder.
 * A valid RowTypeInfo must be properly configured in the builder, e.g.:
 *
 * <pre><code>
 * TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
 *		BasicTypeInfo.INT_TYPE_INFO,
 *		BasicTypeInfo.STRING_TYPE_INFO,
 *		BasicTypeInfo.STRING_TYPE_INFO,
 *		BasicTypeInfo.DOUBLE_TYPE_INFO,
 *		BasicTypeInfo.INT_TYPE_INFO
 *	};
 *
 * RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
 *
 * JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
 *				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
 *				.setDBUrl("jdbc:derby:memory:ebookshop")
 *				.setQuery("select * from books")
 *				.setRowTypeInfo(rowTypeInfo)
 *				.finish();
 * </code></pre>
 *
 * <p>In order to query the JDBC source in parallel, you need to provide a
 * parameterized query template (i.e. a valid {@link PreparedStatement}) and
 * a {@link ParameterValuesProvider} which provides binding values for the
 * query parameters. E.g.:
 *
 * <pre><code>
 *
 * Serializable[][] queryParameters = new String[2][1];
 * queryParameters[0] = new String[]{"Kumar"};
 * queryParameters[1] = new String[]{"Tan Ah Teck"};
 *
 * JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
 *				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
 *				.setDBUrl("jdbc:derby:memory:ebookshop")
 *				.setQuery("select * from books WHERE author = ?")
 *				.setRowTypeInfo(rowTypeInfo)
 *				.setParametersProvider(new GenericParameterValuesProvider(queryParameters))
 *				.finish();
 * </code></pre>
 *
 * @see Row
 * @see ParameterValuesProvider
 * @see PreparedStatement
 * @see DriverManager
 *
 * @deprecated Please use {@link JdbcInputFormat}.
 */
@Deprecated
public class JDBCInputFormat extends JdbcInputFormat {

	public JDBCInputFormat() {}

	public static JDBCInputFormatBuilder buildJDBCInputFormat() {
		return new JDBCInputFormatBuilder();
	}

	/**
	 * A builder used to set parameters to the output format's configuration in a fluent way.
	 */
	public static class JDBCInputFormatBuilder {

		private final JDBCInputFormat format;

		public JDBCInputFormatBuilder() {
			this.format = new JDBCInputFormat();
			//using TYPE_FORWARD_ONLY for high performance reads
			this.format.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
			this.format.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
		}

		public JDBCInputFormatBuilder setUsername(String username) {
			format.username = username;
			return this;
		}

		public JDBCInputFormatBuilder setPassword(String password) {
			format.password = password;
			return this;
		}

		public JDBCInputFormatBuilder setDrivername(String drivername) {
			format.drivername = drivername;
			return this;
		}

		public JDBCInputFormatBuilder setDBUrl(String dbURL) {
			format.dbURL = dbURL;
			return this;
		}

		public JDBCInputFormatBuilder setQuery(String query) {
			format.queryTemplate = query;
			return this;
		}

		public JDBCInputFormatBuilder setResultSetType(int resultSetType) {
			format.resultSetType = resultSetType;
			return this;
		}

		public JDBCInputFormatBuilder setResultSetConcurrency(int resultSetConcurrency) {
			format.resultSetConcurrency = resultSetConcurrency;
			return this;
		}

		public JDBCInputFormatBuilder setParametersProvider(JdbcParameterValuesProvider parameterValuesProvider) {
			format.parameterValues = parameterValuesProvider.getParameterValues();
			return this;
		}

		public JDBCInputFormatBuilder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
			format.rowTypeInfo = rowTypeInfo;
			return this;
		}

		public JDBCInputFormatBuilder setFetchSize(int fetchSize) {
			Preconditions.checkArgument(fetchSize == Integer.MIN_VALUE || fetchSize > 0,
				"Illegal value %s for fetchSize, has to be positive or Integer.MIN_VALUE.", fetchSize);
			format.fetchSize = fetchSize;
			return this;
		}

		public JDBCInputFormatBuilder setAutoCommit(Boolean autoCommit) {
			format.autoCommit = autoCommit;
			return this;
		}

		public JDBCInputFormat finish() {
			if (format.username == null) {
				LOG.info("Username was not supplied separately.");
			}
			if (format.password == null) {
				LOG.info("Password was not supplied separately.");
			}
			if (format.dbURL == null) {
				throw new IllegalArgumentException("No database URL supplied");
			}
			if (format.queryTemplate == null) {
				throw new IllegalArgumentException("No query supplied");
			}
			if (format.drivername == null) {
				throw new IllegalArgumentException("No driver supplied");
			}
			if (format.rowTypeInfo == null) {
				throw new IllegalArgumentException("No " + RowTypeInfo.class.getSimpleName() + " supplied");
			}
			if (format.parameterValues == null) {
				LOG.debug("No input splitting configured (data will be read with parallelism 1).");
			}
			return format;
		}
	}

	@Override
	@VisibleForTesting
	protected PreparedStatement getStatement() {
		return super.getStatement();
	}

	@Override
	@VisibleForTesting
	protected Connection getDbConn() {
		return super.getDbConn();
	}
}
