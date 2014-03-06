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
package eu.stratosphere.api.java.record.io.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.io.UnsplittableInput;
import eu.stratosphere.api.java.record.io.GenericInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.BooleanValue;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.NullValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.ShortValue;
import eu.stratosphere.types.StringValue;

/**
 * InputFormat to read data from a database and generate PactReords.
 * The InputFormat has to be configured with the query, and either all
 * connection parameters or a complete database URL.{@link Configuration} The position of a value inside a Record is
 * determined by the table
 * returned.
 * 
 * @see Configuration
 * @see Record
 * @see DriverManager
 */
public class JDBCInputFormat extends GenericInputFormat implements UnsplittableInput {

	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(JDBCInputFormat.class);
	

	public final String DRIVER_KEY = "driver";
	public final String USERNAME_KEY = "username";
	public final String PASSWORD_KEY = "password";
	public final String URL_KEY = "url";
	public final String QUERY_KEY = "query";


	private String username;
	private String password;
	private String driverName;
	private String dbURL;
	private String query;

	
	private transient Connection dbConn;
	private transient Statement statement;
	private transient ResultSet resultSet;


	/**
	 * Creates a non-configured JDBCInputFormat. This format has to be
	 * configured using configure(configuration).
	 */
	public JDBCInputFormat() {}

	/**
	 * Creates a JDBCInputFormat and configures it.
	 * 
	 * @param driverName
	 *        JDBC-Drivename
	 * @param dbURL
	 *        Formatted URL containing all connection parameters.
	 * @param username
	 * @param password
	 * @param query
	 *        Query to execute.
	 */
	public JDBCInputFormat(String driverName, String dbURL, String username, String password, String query) {
		this.driverName = driverName;
		this.query = query;
		this.dbURL = dbURL;
		this.username = username;
		this.password = password;
	}

	/**
	 * Creates a JDBCInputFormat and configures it.
	 * 
	 * @param driverName
	 *        JDBC-Drivername
	 * @param dbURL
	 *        Formatted URL containing all connection parameters.
	 * @param query
	 *        Query to execute.
	 */
	public JDBCInputFormat(String driverName, String dbURL, String query) {
		this(driverName, dbURL, "", "", query);
	}

	/**
	 * Creates a JDBCInputFormat and configures it.
	 * 
	 * @param parameters
	 *        Configuration with all connection parameters.
	 * @param query
	 *        Query to execute.
	 */
	public JDBCInputFormat(Configuration parameters, String query) {
		this.driverName = parameters.getString(DRIVER_KEY, "");
		this.username = parameters.getString(USERNAME_KEY, "");
		this.password = parameters.getString(PASSWORD_KEY, "");
		this.dbURL = parameters.getString(URL_KEY, "");
		this.query = query;
	}

	
	/**
	 * Configures this JDBCInputFormat. This includes setting the connection
	 * parameters (if necessary), establishing the connection and executing the
	 * query.
	 * 
	 * @param parameters
	 *        Configuration containing all or no parameters.
	 */
	@Override
	public void configure(Configuration parameters) {
		boolean needConfigure = isFieldNullOrEmpty(this.query) || isFieldNullOrEmpty(this.dbURL);
		if (needConfigure) {
			this.driverName = parameters.getString(DRIVER_KEY, null);
			this.username = parameters.getString(USERNAME_KEY, null);
			this.password = parameters.getString(PASSWORD_KEY, null);
			this.query = parameters.getString(QUERY_KEY, null);
			this.dbURL = parameters.getString(URL_KEY, null);
		}

		try {
			prepareQueryExecution();
		} catch (SQLException e) {
			throw new IllegalArgumentException("Configure failed:\t!", e);
		}
	}

	/**
	 * Enters data value from the current resultSet into a Record.
	 * 
	 * @param pos
	 *        Record position to be set.
	 * @param type
	 *        SQL type of the resultSet value.
	 * @param record
	 *        Target Record.
	 */
	private void retrieveTypeAndFillRecord(int pos, int type, Record record) throws SQLException,
			NotTransformableSQLFieldException {
		switch (type) {
		case java.sql.Types.NULL:
			record.setField(pos, NullValue.getInstance());
			break;
		case java.sql.Types.BOOLEAN:
			record.setField(pos, new BooleanValue(resultSet.getBoolean(pos + 1)));
			break;
		case java.sql.Types.BIT:
			record.setField(pos, new BooleanValue(resultSet.getBoolean(pos + 1)));
			break;
		case java.sql.Types.CHAR:
			record.setField(pos, new StringValue(resultSet.getString(pos + 1)));
			break;
		case java.sql.Types.NCHAR:
			record.setField(pos, new StringValue(resultSet.getString(pos + 1)));
			break;
		case java.sql.Types.VARCHAR:
			record.setField(pos, new StringValue(resultSet.getString(pos + 1)));
			break;
		case java.sql.Types.LONGVARCHAR:
			record.setField(pos, new StringValue(resultSet.getString(pos + 1)));
			break;
		case java.sql.Types.LONGNVARCHAR:
			record.setField(pos, new StringValue(resultSet.getString(pos + 1)));
			break;
		case java.sql.Types.TINYINT:
			record.setField(pos, new ShortValue(resultSet.getShort(pos + 1)));
			break;
		case java.sql.Types.SMALLINT:
			record.setField(pos, new ShortValue(resultSet.getShort(pos + 1)));
			break;
		case java.sql.Types.BIGINT:
			record.setField(pos, new LongValue(resultSet.getLong(pos + 1)));
			break;
		case java.sql.Types.INTEGER:
			record.setField(pos, new IntValue(resultSet.getInt(pos + 1)));
			break;
		case java.sql.Types.FLOAT:
			record.setField(pos, new DoubleValue(resultSet.getDouble(pos + 1)));
			break;
		case java.sql.Types.REAL:
			record.setField(pos, new FloatValue(resultSet.getFloat(pos + 1)));
			break;
		case java.sql.Types.DOUBLE:
			record.setField(pos, new DoubleValue(resultSet.getDouble(pos + 1)));
			break;
		case java.sql.Types.DECIMAL:
			record.setField(pos, new DoubleValue(resultSet.getBigDecimal(pos + 1).doubleValue()));
			break;
		case java.sql.Types.NUMERIC:
			record.setField(pos, new DoubleValue(resultSet.getBigDecimal(pos + 1).doubleValue()));
			break;
		case java.sql.Types.DATE:
			record.setField(pos, new StringValue(resultSet.getDate(pos + 1).toString()));
			break;
		case java.sql.Types.TIME:
			record.setField(pos, new LongValue(resultSet.getTime(pos + 1).getTime()));
			break;
		case java.sql.Types.TIMESTAMP:
			record.setField(pos, new StringValue(resultSet.getTimestamp(pos + 1).toString()));
			break;
		case java.sql.Types.SQLXML:
			record.setField(pos, new StringValue(resultSet.getSQLXML(pos + 1).toString()));
			break;
		default:
			throw new NotTransformableSQLFieldException("Unknown sql-type [" + type + "]on column [" + pos + "]");

			// case java.sql.Types.BINARY:
			// case java.sql.Types.VARBINARY:
			// case java.sql.Types.LONGVARBINARY:
			// case java.sql.Types.ARRAY:
			// case java.sql.Types.JAVA_OBJECT:
			// case java.sql.Types.BLOB:
			// case java.sql.Types.CLOB:
			// case java.sql.Types.NCLOB:
			// case java.sql.Types.DATALINK:
			// case java.sql.Types.DISTINCT:
			// case java.sql.Types.OTHER:
			// case java.sql.Types.REF:
			// case java.sql.Types.ROWID:
			// case java.sql.Types.STRUCT:
		}
	}

	private boolean isFieldNullOrEmpty(String field) {
		return (field == null || field.length() == 0);
	}

	private void prepareQueryExecution() throws SQLException {
		setClassForDBType();
		prepareCredentialsAndExecute();
	}

	/**
	 * Loads appropriate JDBC driver.
	 * 
	 * @param dbType
	 *        Type of the database.
	 * @return boolean value, indication whether an appropriate driver could be
	 *         found.
	 */
	private void setClassForDBType() {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC-Class not found:\t" + cnfe.getLocalizedMessage());
		}
	}

	private void prepareCredentialsAndExecute() throws SQLException {
		if (isFieldNullOrEmpty(username)) {
			prepareConnection(dbURL);
		} else {
			prepareConnection();
		}
		executeQuery();
	}

	/**
	 * Establishes a connection to a database.
	 * 
	 * @param dbURL
	 *        Assembled URL containing all connection parameters.
	 * @return boolean value, indicating whether a connection could be
	 *         established
	 */
	private void prepareConnection(String dbURL) throws SQLException {
		dbConn = DriverManager.getConnection(dbURL);
	}

	/**
	 * Assembles the Database URL and establishes a connection.
	 * 
	 * @param dbType
	 *        Type of the database.
	 * @param username
	 *        Login username.
	 * @param password
	 *        Login password.
	 * @return boolean value, indicating whether a connection could be
	 *         established
	 */
	private void prepareConnection() throws SQLException {
		dbConn = DriverManager.getConnection(dbURL, username, password);
	}

	private void executeQuery() throws SQLException {
		statement = dbConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		resultSet = statement.executeQuery(this.query);
	}

	/**
	 * Checks whether all data has been read.
	 * 
	 * @return boolean value indication whether all data has been read.
	 */
	@Override
	public boolean reachedEnd() {
		try {
			if (resultSet.isLast()) {
				resultSet.close();
				statement.close();
				dbConn.close();
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			throw new IllegalArgumentException("Couldn't evaluate reachedEnd():\t" + e.getMessage());
		} catch (NullPointerException e) {
			throw new IllegalArgumentException("Couldn't access resultSet:\t" + e.getMessage());
		}
	}

	/**
	 * Stores the next resultSet row in a Record
	 * 
	 * @param record
	 *        target Record
	 * @return boolean value indicating that the operation was successful
	 */
	@Override
	public Record nextRecord(Record record) {
		try {
			resultSet.next();
			ResultSetMetaData rsmd = resultSet.getMetaData();
			int column_count = rsmd.getColumnCount();
			record.setNumFields(column_count);

			for (int pos = 0; pos < column_count; pos++) {
				int type = rsmd.getColumnType(pos + 1);
				retrieveTypeAndFillRecord(pos, type, record);
			}
			return record;
		} catch (SQLException e) {
			throw new IllegalArgumentException("Couldn't read data:\t" + e.getMessage());
		} catch (NotTransformableSQLFieldException e) {
			throw new IllegalArgumentException("Couldn't read data because of unknown column sql-type:\t"
				+ e.getMessage());
		} catch (NullPointerException e) {
			throw new IllegalArgumentException("Couldn't access resultSet:\t" + e.getMessage());
		}
	}
	
	public static class NotTransformableSQLFieldException extends Exception {

		private static final long serialVersionUID = 1L;

		public NotTransformableSQLFieldException(String message) {
			super(message);
		}
	}

}
