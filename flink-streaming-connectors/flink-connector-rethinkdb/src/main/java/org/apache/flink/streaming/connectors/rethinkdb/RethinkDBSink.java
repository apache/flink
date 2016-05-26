/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.rethinkdb;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.net.Connection;

/**
 * This class is the Flink sink for RethinkDB which is a tcp/JSON protocol based document
 * oriented NoSQL database.
 * 
 * <p/>
 * This sink provides two constuctors:
 * <p/>
 * {@link #FlinkRethinkDbSink(String hostname, int hostport, String database, String table, JSONSerializationSchema schema)}, and
 * <p/>
 * {@link #FlinkRethinkDbSink(String hostname, int hostport, String database, String table, JSONSerializationSchema schema, ConflictStrategy conflictStrategy)}
 * <p/>
 * 
 * The parameter for the constructor are as follows:
 * <p/>
 * <ul>
 * <li>hostname - the rethinkdb hostname</li>
 * <li>hosport - the rethinkdb port for the driver to connect</li>
 * <li>database - the rethinkdb database name to which the table belongs</li>
 * <li>table - the rethinkdb table name where documents are inserted</li>
 * <li>schema - the schema tranfromer that converts input to JSONObject, or JSONArray</li>
 * <li>conflictStrategy - the conflict resolution strategy in case inserted document has id which exists in the db</li>
 * </ul>
 * <p/>
 *
 * The user can also set:
 * <p/>
 * <ul>
 * <li>username - default is admin</li>
 * <li>password - default is blank</li>
 * </ul>
 * <p/> with the {@link #setUsernameAndPassword(String, String)} method.
 * <p/>
 * <b>NOTE: If multiple documents are getting inserted (eg: using JSONArray), the sink 
 * checks if there is an error entry in the result HashMap and throws a runtime exception if errors
 * counts is not zero.  The exception message contains the results HashMap. 
 * In case of multiple errors only the first error is noted in the result HashMap.
 * </b>
 * 
 * @see {@link ConflictStrategy} for conflict resolution strategies
 * 
 * @param <OUT> a value that can be transformed into a {@link org.json.simple.JSONArray;} or {@link org.json.simple.JSONObject}
 */
public class RethinkDBSink<OUT> extends RichSinkFunction<OUT> implements Serializable{

	/**
	 * Serial version for the class
	 */
	private static final long serialVersionUID = -2135499016796158755L;

	/**
	 * Logger for the class
	 */
	private static final Logger LOG = LoggerFactory.getLogger(RethinkDBSink.class);

	/**
	 * Conflict resolution option key in case document ids are same 
	 */
	public static final String CONFLICT_OPT = "conflict";

	/**
	 * Result key indicating number of errors
	 */
	public static final String RESULT_ERROR_KEY = "errors";

	/**
	 * Serialization schema for the sink
	 */
	private JSONSerializationSchema<OUT> serializationSchema;

	/**
	 * RethinkDB connection object
	 */
	private transient Connection rethinkDbConnection;

	/**
	 * RethinkDB hostname
	 */
	private String hostname;

	/**
	 * RethinkDB port
	 */
	private int hostport;

	/**
	 * RethinkDB tablename where documents are inserted
	 */
	private String tableName;

	/**
	 * RethinkDB database where document are inserted
	 */
	private String databaseName;

	/**
	 * Conflict resolution strategy
	 */
	private ConflictStrategy conflict;
	
	/**
	 * Default user name
	 */
	public static final String DEFAULT_USER_NAME = "admin";
	
	/**
	 * User name
	 */
	private String username = DEFAULT_USER_NAME;
	
	/**
	 * Default user name
	 */
	public static final String DEFAULT_PASSWORD = "";
	
	/**
	 * password
	 */
	private String password = DEFAULT_PASSWORD;

	/**
	 * Set durability
	 * 
	 * @param durability
	 * 
	 * @see #Durability
	 */
	public void setDurability(Durability durability) {
		this.durability = durability;
	}

	/**
	 * Durability configuration
	 */
	protected Durability durability = Durability.hard;
	
	/**
	 * Constructor for RethinkDB sink
	 * @param hostname
	 * @param hostport
	 * @param database
	 * @param table
	 * @param schema
	 */
	public RethinkDBSink(String hostname, int hostport, String database, String table, 
			JSONSerializationSchema<OUT> schema) {
		this(hostname, hostport, database, table, schema, ConflictStrategy.update);
	}

	/**
	 * Constructor for sink
	 * @param hostname
	 * @param hostport
	 * @param database name
	 * @param table name
	 * @param schema serialization converter
	 * @param conflict resolution strategy for document id conflict
	 */
	public RethinkDBSink(String hostname, int hostport, String database, String table, 
			JSONSerializationSchema<OUT> schema, 
			ConflictStrategy conflict) {
		this.hostname = Objects.requireNonNull(hostname);
		this.hostport = hostport;
		this.databaseName = Objects.requireNonNull(database);
		this.tableName = Objects.requireNonNull(table);
		this.serializationSchema = Objects.requireNonNull(schema);
		this.conflict = conflict;
	}

	/**
	 * Open the sink
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		LOG.info("Received parameters : {}", parameters);
		
		super.open(parameters);

		rethinkDbConnection = getRethinkDB().connection().hostname(hostname)
				.port(hostport).user(username, password).connect();

		LOG.info("RethinkDb connection created for host {} port {} and db {}", 
				hostname, hostport,databaseName);
	}

	/**
	 * Helper method to help testability
	 * @return RethinkDB instance
	 */ 
	protected RethinkDB getRethinkDB() {
		return RethinkDB.r;
	}
	
	/**
	 * Set username and password. If username and password are not provided,
	 * then default username (admin) and blank password are used.
	 * 
	 * @param username cannot be blank/null
	 * @param password cannot be null
	 * 
	 * @throws IllegalArgumentException if arguments is null or empty
	 */
	public void setUsernameAndPassword(String username, String password) {
		
		if ( StringUtils.isBlank(username) )  {
			throw new IllegalArgumentException("username " + username + " cannot be null or empty" ); 
		} else {
			this.username = username;
		}
		
		this.password = (password == null) ? "" : password;
	}
	
	/**
	 * Invoke the sink with the input
	 * 
	 * @param the value to be inserted
	 * 
	 * @throws RuntimeException if there are errors while inserting row into rethinkdb
	 */
	@Override
	public void invoke(OUT value) throws Exception {
		LOG.debug("Received value {}", value);
		
		Object json = serializationSchema.toJSON(value);
		LOG.debug("Object/Json: {}/{}", value, json);
		Insert insert = getRdbTable().insert(json)
				.optArg(CONFLICT_OPT, conflict.toString())
				.optArg("durability", durability.name());
		HashMap<String,Object> result = runInsert(insert);
		
		LOG.debug("Object/Json/Result: {}/{}/{}", value, json, result);
		
		if ( (Long)result.get(RESULT_ERROR_KEY) != 0 ) {
			LOG.error("There were errors while inserting data value/result {}/{}", value, result);
			throw new RuntimeException("Errors " + result + " while inserting " + value );
		}
	}

	protected HashMap<String,Object> runInsert(Insert insert) {
		return insert.run(rethinkDbConnection);
	}

	/**
	 * Close the sink
	 */
	@Override
	public void close() throws Exception {
		LOG.info("Closing connection");
		
		rethinkDbConnection.close();
		
		super.close();
	}

	/**
	 * @return the rethinkDbConnection
	 */
	protected Connection getRethinkDbConnection() {
		return rethinkDbConnection;
	}

	/**
	 * @return the rdbTable
	 */
	protected Table getRdbTable() {
		return getRethinkDB().db(databaseName).table(tableName);
	}

	/**
	 * @return the hostname
	 */
	public String getHostname() {
		return hostname;
	}

	/**
	 * @return the hostport
	 */
	public int getHostport() {
		return hostport;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @return the databaseName
	 */
	public String getDatabaseName() {
		return databaseName;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

}