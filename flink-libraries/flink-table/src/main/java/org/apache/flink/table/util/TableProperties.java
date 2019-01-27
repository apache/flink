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

package org.apache.flink.table.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Properties utils for a table.
 */
public class TableProperties extends Configuration {
	private static final Logger LOG = LoggerFactory.getLogger(TableProperties.class);

	public static final String TABLE_NAME = "__tablename__".toLowerCase();
	public static final String BLINK_ENVIRONMENT_TYPE_KEY = "blinkEnvironmentTypeKey".toLowerCase();
	public static final String BLINK_ENVIRONMENT_STREAM_VALUE = "stream".toLowerCase();
	public static final String BLINK_ENVIRONMENT_BATCHEXEC_VALUE = "batchExec".toLowerCase();
	public static final String BLINK_CONNECTOR_TYPE_KEY = "type".toLowerCase();

	/** Internal keys for excluding supported keys. **/
	public static final List<String> INTERNAL_KEYS = Arrays.asList(TABLE_NAME,
		BLINK_ENVIRONMENT_TYPE_KEY,
		BLINK_ENVIRONMENT_STREAM_VALUE,
		BLINK_ENVIRONMENT_BATCHEXEC_VALUE,
		BLINK_CONNECTOR_TYPE_KEY,
		SchemaValidator.SCHEMA());

	/**
	 * Returns a new TableProperties with lower case keys.
	 */
	public TableProperties toKeyLowerCase() {
		TableProperties ret = new TableProperties();
		synchronized (this.confData) {
			for (Map.Entry<String, Object> entry : this.confData.entrySet()) {
				ret.setString(entry.getKey().toLowerCase(), entry.getValue().toString());
			}
			return ret;
		}
	}

	public TableProperties putProperties(Map<String, String> properties) {
		for (Map.Entry<String, String> property : properties.entrySet()) {
			setString(property.getKey(), property.getValue());
		}
		return this;
	}

	public TableProperties property(String key, String value) {
		this.setString(key, value);
		return this;
	}

	public void putSchemaIntoProperties(RichTableSchema schema) {
		try {
			byte[] serialized = InstantiationUtil.serializeObject(schema);
			String encoded = Base64.getEncoder().encodeToString(serialized);
			setString(SchemaValidator.SCHEMA(), encoded);
		} catch (IOException ioe) {
			LOG.error("Exception when put rich table schema to configuration: {}", ioe.getCause());
			throw new RuntimeException(ioe.getMessage());
		}
	}

	public RichTableSchema readSchemaFromProperties(ClassLoader classLoader) {
		try {
			String encoded = getString(SchemaValidator.SCHEMA(), null);
			return InstantiationUtil.deserializeObject(Base64.getDecoder().decode(encoded),
				classLoader);
		} catch (ClassNotFoundException | IOException cne) {
			LOG.error("Exception when put rich table schema to configuration: {}", cne.getCause());
			throw new RuntimeException(cne.getMessage());
		}
	}

	public void putTableNameIntoProperties(String tableName) {
		Preconditions.checkArgument(tableName != null);
		setString(TABLE_NAME, tableName);
	}

	public String readTableNameFromProperties() {
		return getString(TABLE_NAME, null);
	}
}
