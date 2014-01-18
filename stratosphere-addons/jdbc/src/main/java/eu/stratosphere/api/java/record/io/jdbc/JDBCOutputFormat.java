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
package eu.stratosphere.api.java.record.io.jdbc;

import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.*;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JDBCOutputFormat implements OutputFormat<Record> {
	private static final long serialVersionUID = 1L;

	private static final int DEFAULT_BATCH_INTERVERAL = 5000;
	
    public static final String DRIVER_KEY = "driver";
    public static final String USERNAME_KEY = "username";
    public static final String PASSWORD_KEY = "password";
    public static final String URL_KEY = "url";
    public static final String QUERY_KEY = "query";
    public static final String FIELD_COUNT_KEY = "fields";
    public static final String FIELD_TYPE_KEY = "type";
    public static final String BATCH_INTERVAL = "batchInt";

    private Connection dbConn;
    private PreparedStatement upload;

    private String username;
    private String password;
    private String driverName;
    private String dbURL;

    private String query;
    private int fieldCount;
    private Class<? extends Value>[] fieldClasses;
    
    /**
     * Variable indicating the current number of insert sets in a batch.
     */
    private int batchCount = 0;
    
    /**
     * Commit interval of batches.
     * High batch interval: faster inserts, more memory required (reduce if OutOfMemoryExceptions occur)
     * low batch interval: slower inserts, less memory.
     */
    private int batchInterval = DEFAULT_BATCH_INTERVERAL;
    

    /**
     * Configures this JDBCOutputFormat.
     * 
     * @param parameters
     *        Configuration containing all parameters.
     */
    @Override
    public void configure(Configuration parameters) {
        this.driverName = parameters.getString(DRIVER_KEY, null);
        this.username = parameters.getString(USERNAME_KEY, null);
        this.password = parameters.getString(PASSWORD_KEY, null);
        this.dbURL = parameters.getString(URL_KEY, null);
        this.query = parameters.getString(QUERY_KEY, null);
        this.fieldCount = parameters.getInteger(FIELD_COUNT_KEY, 0);
        this.batchInterval = parameters.getInteger(BATCH_INTERVAL, DEFAULT_BATCH_INTERVERAL);

        @SuppressWarnings("unchecked")
        Class<Value>[] classes = new Class[this.fieldCount];
        this.fieldClasses = classes;

        for (int i = 0; i < this.fieldCount; i++) {
            @SuppressWarnings("unchecked")
            Class<? extends Value> clazz = (Class<? extends Value>) parameters.getClass(FIELD_TYPE_KEY + i, null);
            if (clazz == null) {
                throw new IllegalArgumentException("Invalid configuration for JDBCOutputFormat: "
                        + "No type class for parameter " + i);
            }
            this.fieldClasses[i] = clazz;
        }
    }

    /**
     * Connects to the target database and initializes the prepared statement.
     *
     * @param taskNumber The number of the parallel instance.
     * @throws IOException Thrown, if the output could not be opened due to an
     * I/O problem.
     */
    @Override
    public void open(int taskNumber) throws IOException {
        try {
            establishConnection();
            upload = dbConn.prepareStatement(query);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed:\t!", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC-Class not found:\t", cnfe);
        }
    }

    private void establishConnection() throws SQLException, ClassNotFoundException {
        Class.forName(driverName);
        if (username == null) {
            dbConn = DriverManager.getConnection(dbURL);
        } else {
            dbConn = DriverManager.getConnection(dbURL, username, password);
        }
    }

    /**
     * Adds a record to the prepared statement.
     * <p>
     * When this method is called, the output format is guaranteed to be opened.
     *
     * @param record The records to add to the output.
     * @throws IOException Thrown, if the records could not be added due to an
     * I/O problem.
     */
    
    @Override
    public void writeRecord(Record record) throws IOException {
        try {
            for (int x = 0; x < record.getNumFields(); x++) {
                Value temp = record.getField(x, fieldClasses[x]);
                addValue(x + 1, temp);
            }
            upload.addBatch();
            batchCount++;
            if(batchCount >= batchInterval) {
            	upload.executeBatch();
            	batchCount = 0;
            }
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("writeRecord() failed:\t", sqe);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("writeRecord() failed:\t", iae);
        }
    }

    private enum pactType {
        BooleanValue,
        ByteValue,
        CharValue,
        DoubleValue,
        FloatValue,
        IntValue,
        LongValue,
        ShortValue,
        StringValue
    }

    private void addValue(int index, Value value) throws SQLException {
        pactType type;
        try {
            type = pactType.valueOf(value.getClass().getSimpleName());
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("PactType not supported:\t", iae);
        }
        switch (type) {
            case BooleanValue:
                upload.setBoolean(index, ((BooleanValue) value).getValue());
                break;
            case ByteValue:
                upload.setByte(index, ((ByteValue) value).getValue());
                break;
            case CharValue:
                upload.setString(index, String.valueOf(((CharValue) value).getValue()));
                break;
            case DoubleValue:
                upload.setDouble(index, ((DoubleValue) value).getValue());
                break;
            case FloatValue:
                upload.setFloat(index, ((FloatValue) value).getValue());
                break;
            case IntValue:
                upload.setInt(index, ((IntValue) value).getValue());
                break;
            case LongValue:
                upload.setLong(index, ((LongValue) value).getValue());
                break;
            case ShortValue:
                upload.setShort(index, ((ShortValue) value).getValue());
                break;
            case StringValue:
                upload.setString(index, ((StringValue) value).getValue());
                break;
        }
    }

    /**
     * Executes prepared statement and closes all resources of this instance.
     *
     * @throws IOException Thrown, if the input could not be closed properly.
     */
    @Override
    public void close() throws IOException {
        try {
            upload.executeBatch();
            batchCount = 0;
            upload.close();
            dbConn.close();
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("close() failed:\t", sqe);
        }
    }

    /**
     * Creates a configuration builder that can be used to set the 
     * output format's parameters to the config in a fluent fashion.
     * 
     * @return A config builder for setting parameters.
     */
    public static ConfigBuilder configureOutputFormat(GenericDataSink target) {
        return new ConfigBuilder(target.getParameters());
    }

    /**
     * Abstract builder used to set parameters to the output format's 
     * configuration in a fluent way.
     */
    protected static abstract class AbstractConfigBuilder<T>
            extends FileOutputFormat.AbstractConfigBuilder<T> {

        /**
         * Creates a new builder for the given configuration.
         * 
         * @param config The configuration into which the parameters will be written.
         */
        protected AbstractConfigBuilder(Configuration config) {
            super(config);
        }

        /**
         * Sets the query field.
         * @param value value to be set.
         * @return The builder itself.
         */
        public T setQuery(String value) {
            this.config.setString(QUERY_KEY, value);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        /**
         * Sets the url field.
         * @param value value to be set.
         * @return The builder itself.
         */
        public T setUrl(String value) {
            this.config.setString(URL_KEY, value);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        /**
         * Sets the username field.
         * @param value value to be set.
         * @return The builder itself.
         */
        public T setUsername(String value) {
            this.config.setString(USERNAME_KEY, value);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        /**
         * Sets the password field.
         * @param value value to be set.
         * @return The builder itself.
         */
        public T setPassword(String value) {
            this.config.setString(PASSWORD_KEY, value);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        /**
         * Sets the driver field.
         * @param value value to be set.
         * @return The builder itself.
         */
        public T setDriver(String value) {
            this.config.setString(DRIVER_KEY, value);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        /**
         * Sets the type of a column.
         * Types are applied in the order they were set.
         * @param type PactType to apply.
         * @return The builder itself.
         */
        public T setClass(Class<? extends Value> type) {
            final int numYet = this.config.getInteger(FIELD_COUNT_KEY, 0);
            this.config.setClass(FIELD_TYPE_KEY + numYet, type);
            this.config.setInteger(FIELD_COUNT_KEY, numYet + 1);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }
    }

    /**
     * A builder used to set parameters to the output format's configuration in a fluent way.
     */
    public static final class ConfigBuilder extends AbstractConfigBuilder<ConfigBuilder> {
        /**
         * Creates a new builder for the given configuration.
         * 
         * @param targetConfig The configuration into which the parameters will be written.
         */
        protected ConfigBuilder(Configuration targetConfig) {
            super(targetConfig);
        }
    }
}
