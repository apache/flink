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
package org.apache.flink.api.java.utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ParameterTool extends ExecutionConfig.GlobalJobParameters implements Serializable, Cloneable {
	protected static String NO_VALUE_KEY = "__NO_VALUE_KEY";
	protected static String DEFAULT_UNDEFINED = "<undefined>";


	// ------------------ Constructors ------------------------

	public static ParameterTool fromArgs(String[] args) {
		Map<String, String> map = new HashMap<String, String>(args.length / 2);

		String key = null;
		String value = null;
		boolean expectValue = false;
		for(String arg: args) {
			// check for -- argument
			if(arg.startsWith("--")) {
				if(expectValue) {
					// we got into a new key, even though we were a value --> current key is one without value
					if(value != null) {
						throw new IllegalStateException("Unexpected state");
					}
					map.put(key, NO_VALUE_KEY);
					// key will be overwritten in the next step
				}
				key = arg.substring(2);
				expectValue = true;
			} // check for - argument
			else if(arg.startsWith("-")) {
				// we are waiting for a value, so this is a - prefixed value (negative number)
				if(expectValue) {

					if(NumberUtils.isNumber(arg)) {
						// negative number
						value = arg;
						expectValue = false;
					} else {
						if(value != null) {
							throw new IllegalStateException("Unexpected state");
						}
						// We waited for a value but found a new key. So the previous key doesnt have a value.
						map.put(key, NO_VALUE_KEY);
						key = arg.substring(1);
						expectValue = true;
					}
				} else {
					// we are not waiting for a value, so its an argument
					key = arg.substring(1);
					expectValue = true;
				}
			} else {
				if(expectValue) {
					value = arg;
					expectValue = false;
				} else {
					throw new RuntimeException("Error parsing arguments '"+ Arrays.toString(args)+"' on '"+arg+"'. Unexpected value. Please prefix values with -- or -.");
				}
			}

			if(value == null && key == null) {
				throw new IllegalStateException("Value and key can not be null at the same time");
			}
			if(key != null && value == null && !expectValue) {
				throw new IllegalStateException("Value expected but flag not set");
			}
			if(key != null && value != null) {
				map.put(key, value);
				key = null;
				value = null;
				expectValue = false;
			}
			if(key != null && key.length() == 0) {
				throw new IllegalArgumentException("The input "+Arrays.toString(args)+" contains an empty argument");
			}

			if(key != null && !expectValue) {
				map.put(key, NO_VALUE_KEY);
				key = null;
				expectValue = false;
			}
		}
		if(key != null) {
			map.put(key, NO_VALUE_KEY);
		}

		return fromMap(map);
	}

	public static ParameterTool fromPropertiesFile(String path) throws IOException {
		File propertiesFile = new File(path);
		if(!propertiesFile.exists()) {
			throw new FileNotFoundException("Properties file "+path+" does not exist");
		}
		Properties props = new Properties();
		props.load(new FileInputStream(propertiesFile));
		return fromMap((Map)props);
	}

	public static ParameterTool fromMap(Map<String, String> map) {
		Preconditions.checkNotNull(map, "Unable to initialize from empty map");
		return new ParameterTool(map);
	}

	public static ParameterTool fromSystemProperties() {
		return fromMap((Map) System.getProperties());
	}

	// ------------------ ParameterUtil  ------------------------
	protected final Map<String, String> data;
	protected final HashMap<String, String> defaultData;

	private ParameterTool(Map<String, String> data) {
		this.data = new HashMap<String, String>(data);
		this.defaultData = new HashMap<String, String>();
	}

	// ------------------ Get data from the util ----------------

	public int getNumberOfParameters() {
		return data.size();
	}

	public String get(String key) {
		addToDefaults(key, null);
		return data.get(key);
	}

	public String getRequired(String key) {
		addToDefaults(key, null);
		String value = get(key);
		if(value == null) {
			throw new RuntimeException("No data for required key '"+key+"'");
		}
		return value;
	}


	public String get(String key, String defaultValue) {
		addToDefaults(key, defaultValue);
		String value = get(key);
		if(value == null) {
			return defaultValue;
		} else {
			return value;
		}
	}

	/**
	 * Check if value is set
	 */
	public boolean has(String value) {
		addToDefaults(value, null);
		return data.containsKey(value);
	}

	// -------------- Integer

	public int getInt(String key) {
		addToDefaults(key, null);
		String value = getRequired(key);
		return Integer.valueOf(value);
	}

	public int getLong(String key, int defaultValue) {
		addToDefaults(key, Integer.toString(defaultValue));
		String value = get(key);
		if(value == null) {
			return defaultValue;
		} else {
			return Integer.valueOf(value);
		}
	}

	// -------------- LONG
	/**
	 * Get long value.
	 * The method fails if the key is not specified.
	 * @param key Name of the key
	 * @return
	 */
	public long getLong(String key) {
		addToDefaults(key, null);
		String value = getRequired(key);
		return Long.valueOf(value);
	}

	public long getLong(String key, long defaultValue) {
		addToDefaults(key, Long.toString(defaultValue));
		String value = get(key);
		if(value == null) {
			return defaultValue;
		} else {
			return Long.valueOf(value);
		}
	}

	// -------------- FLOAT

	public float getFloat(String key) {
		addToDefaults(key, null);
		String value = getRequired(key);
		return Float.valueOf(value);
	}

	public float getFloat(String key, float defaultValue) {
		addToDefaults(key, Float.toString(defaultValue));
		String value = get(key);
		if(value == null) {
			return defaultValue;
		} else {
			return Float.valueOf(value);
		}
	}

	// -------------- DOUBLE

	public double getDouble(String key) {
		addToDefaults(key, null);
		String value = getRequired(key);
		return Double.valueOf(value);
	}

	public double getDouble(String key, double defaultValue) {
		addToDefaults(key, Double.toString(defaultValue));
		String value = get(key);
		if(value == null) {
			return defaultValue;
		} else {
			return Double.valueOf(value);
		}
	}

	// --------------- Internals

	protected void addToDefaults(String key, String value) {
		String currentValue = defaultData.get(key);
		if(currentValue == null) {
			if(value == null) {
				value = DEFAULT_UNDEFINED;
			}
			defaultData.put(key, value);
		} else {
			// there is already an entry for this key. Check if the value is the undefined
			if(currentValue.equals(DEFAULT_UNDEFINED) && value != null) {
				// update key with better default value
				defaultData.put(key, value);
			}
		}
	}

	// ------------------------- Export to different targets -------------------------

	public Configuration getConfiguration() {
		Configuration conf = new Configuration();
		for(Map.Entry<String, String> entry: data.entrySet()) {
			conf.setString(entry.getKey(), entry.getValue());
		}
		return conf;
	}

	public Properties getProperties() {
		Properties props = new Properties();
		props.putAll(this.data);
		return props;
	}


	/**
	 * Create a properties file with all the known parameters (call after the last get*() call).
	 * Set the default value, if available.
	 *
	 * Use this method to create a properties file skeleton.
	 *
	 * @param pathToFile Location of the default properties file.
	 */
	public void createPropertiesFile(String pathToFile) throws IOException {
		createPropertiesFile(pathToFile, true);
	}

	public void createPropertiesFile(String pathToFile, boolean overwrite) throws IOException {
		File file = new File(pathToFile);
		if(file.exists()) {
			if(overwrite) {
				file.delete();
			} else {
				throw new RuntimeException("File "+pathToFile+" exists and overwriting is not allowed");
			}
		}
		Properties defaultProps = new Properties();
		defaultProps.putAll(this.defaultData);
		defaultProps.store(new FileOutputStream(file), "Default file created by Flink's ParameterUtil.createPropertiesFile()");
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		return new ParameterTool(this.data);
	}



	// ------------------------- Interaction with other ParameterUtils -------------------------

	public ParameterTool mergeWith(ParameterTool other) {
		ParameterTool ret = new ParameterTool(this.data);
		ret.data.putAll(other.data);
		return ret;
	}

	// ------------------------- ExecutionConfig.UserConfig interface -------------------------

	@Override
	public Map<String, String> toMap() {
		return data;
	}

}
