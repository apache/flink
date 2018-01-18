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

package org.apache.flink.configuration;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A configuration that manages a subset of keys with a common prefix from a given configuration.
 */
public final class DelegatingConfiguration extends Configuration {

	private static final long serialVersionUID = 1L;

	private final Configuration backingConfig;		// the configuration actually storing the data

	private String prefix;							// the prefix key by which keys for this config are marked

	// --------------------------------------------------------------------------------------------

	/**
	 * Default constructor for serialization. Creates an empty delegating configuration.
	 */
	public DelegatingConfiguration() {
		this.backingConfig = new Configuration();
		this.prefix = "";
	}

	/**
	 * Creates a new delegating configuration which stores its key/value pairs in the given
	 * configuration using the specifies key prefix.
	 *
	 * @param backingConfig The configuration holding the actual config data.
	 * @param prefix The prefix prepended to all config keys.
	 */
	public DelegatingConfiguration(Configuration backingConfig, String prefix) {
		this.backingConfig = Preconditions.checkNotNull(backingConfig);
		this.prefix = prefix;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String getString(String key, String defaultValue) {
		return this.backingConfig.getString(this.prefix + key, defaultValue);
	}

	@Override
	public String getString(ConfigOption<String> configOption) {
		return  this.backingConfig.getString(prefixOption(configOption, prefix));
	}

	@Override
	public String getString(ConfigOption<String> configOption, String overrideDefault) {
		return  this.backingConfig.getString(prefixOption(configOption, prefix), overrideDefault);
	}

	@Override
	public void setString(String key, String value) {
		this.backingConfig.setString(this.prefix + key, value);
	}

	@Override
	public void setString(ConfigOption<String> key, String value) {
		this.backingConfig.setString(prefix + key.key(), value);
	}

	@Override
	public <T> Class<T> getClass(String key, Class<? extends T> defaultValue, ClassLoader classLoader) throws ClassNotFoundException {
		return this.backingConfig.getClass(this.prefix + key, defaultValue, classLoader);
	}

	@Override
	public void setClass(String key, Class<?> klazz) {
		this.backingConfig.setClass(this.prefix + key, klazz);
	}

	@Override
	public int getInteger(String key, int defaultValue) {
		return this.backingConfig.getInteger(this.prefix + key, defaultValue);
	}

	@Override
	public int getInteger(ConfigOption<Integer> configOption) {
		return  this.backingConfig.getInteger(prefixOption(configOption, prefix));
	}

	@Override
	public int getInteger(ConfigOption<Integer> configOption, int overrideDefault) {
		return this.backingConfig.getInteger(configOption, overrideDefault);
	}

	@Override
	public void setInteger(String key, int value) {
		this.backingConfig.setInteger(this.prefix + key, value);
	}

	@Override
	public void setInteger(ConfigOption<Integer> key, int value) {
		this.backingConfig.setInteger(prefix + key.key(), value);
	}

	@Override
	public long getLong(String key, long defaultValue) {
		return this.backingConfig.getLong(this.prefix + key, defaultValue);
	}

	@Override
	public long getLong(ConfigOption<Long> configOption) {
		return  this.backingConfig.getLong(prefixOption(configOption, prefix));
	}

	@Override
	public long getLong(ConfigOption<Long> configOption, long overrideDefault) {
		return this.backingConfig.getLong(configOption, overrideDefault);
	}

	@Override
	public void setLong(String key, long value) {
		this.backingConfig.setLong(this.prefix + key, value);
	}

	@Override
	public void setLong(ConfigOption<Long> key, long value) {
		this.backingConfig.setLong(prefix + key.key(), value);
	}

	@Override
	public boolean getBoolean(String key, boolean defaultValue) {
		return this.backingConfig.getBoolean(this.prefix + key, defaultValue);
	}

	@Override
	public boolean getBoolean(ConfigOption<Boolean> configOption) {
		return  this.backingConfig.getBoolean(prefixOption(configOption, prefix));
	}

	@Override
	public void setBoolean(String key, boolean value) {
		this.backingConfig.setBoolean(this.prefix + key, value);
	}

	@Override
	public void setBoolean(ConfigOption<Boolean> key, boolean value) {
		this.backingConfig.setBoolean(prefix + key.key(), value);
	}

	@Override
	public boolean getBoolean(ConfigOption<Boolean> configOption, boolean overrideDefault) {
		return this.backingConfig.getBoolean(configOption, overrideDefault);
	}

	@Override
	public float getFloat(String key, float defaultValue) {
		return this.backingConfig.getFloat(this.prefix + key, defaultValue);
	}

	@Override
	public float getFloat(ConfigOption<Float> configOption) {
		return this.backingConfig.getFloat(prefixOption(configOption, prefix));
	}

	@Override
	public float getFloat(ConfigOption<Float> configOption, float overrideDefault) {
		return this.backingConfig.getFloat(configOption, overrideDefault);
	}

	@Override
	public void setFloat(String key, float value) {
		this.backingConfig.setFloat(this.prefix + key, value);
	}

	@Override
	public void setFloat(ConfigOption<Float> key, float value) {
		this.backingConfig.setFloat(prefix + key.key(), value);
	}

	@Override
	public double getDouble(String key, double defaultValue) {
		return this.backingConfig.getDouble(this.prefix + key, defaultValue);
	}

	@Override
	public double getDouble(ConfigOption<Double> configOption) {
		return this.backingConfig.getDouble(prefixOption(configOption, prefix));
	}

	@Override
	public double getDouble(ConfigOption<Double> configOption, double overrideDefault) {
		return this.backingConfig.getDouble(configOption, overrideDefault);
	}

	@Override
	public void setDouble(String key, double value) {
		this.backingConfig.setDouble(this.prefix + key, value);
	}

	@Override
	public void setDouble(ConfigOption<Double> key, double value) {
		this.backingConfig.setDouble(prefix + key.key(), value);
	}

	@Override
	public byte[] getBytes(final String key, final byte[] defaultValue) {
		return this.backingConfig.getBytes(this.prefix + key, defaultValue);
	}

	@Override
	public void setBytes(final String key, final byte[] bytes) {
		this.backingConfig.setBytes(this.prefix + key, bytes);
	}

	@Override
	public String getValue(ConfigOption<?> configOption) {
		return this.backingConfig.getValue(prefixOption(configOption, prefix));
	}

	@Override
	public void addAllToProperties(Properties props) {
		// only add keys with our prefix
		synchronized (backingConfig.confData) {
			for (Map.Entry<String, Object> entry : backingConfig.confData.entrySet()) {
				if (entry.getKey().startsWith(prefix)) {
					String keyWithoutPrefix =
							entry.getKey().substring(prefix.length(),
									entry.getKey().length());

					props.put(keyWithoutPrefix, entry.getValue());
				} else {
					// don't add stuff that doesn't have our prefix
				}
			}
		}

	}

	@Override
	public void addAll(Configuration other) {
		this.addAll(other, "");
	}

	@Override
	public void addAll(Configuration other, String prefix) {
		this.backingConfig.addAll(other, this.prefix + prefix);
	}

	@Override
	public String toString() {
		return backingConfig.toString();
	}

	@Override
	public Set<String> keySet() {
		if (this.prefix == null) {
			return this.backingConfig.keySet();
		}

		final HashSet<String> set = new HashSet<>();
		int prefixLen = this.prefix.length();

		for (String key : this.backingConfig.keySet()) {
			if (key.startsWith(prefix)) {
				set.add(key.substring(prefixLen));
			}
		}

		return set;
	}

	@Override
	public Configuration clone() {
		return new DelegatingConfiguration(backingConfig.clone(), prefix);
	}

	@Override
	public Map<String, String> toMap() {
		Map<String, String> map = backingConfig.toMap();
		Map<String, String> prefixed = new HashMap<>(map.size());
		for (Map.Entry<String, String> entry : map.entrySet()) {
			prefixed.put(prefix + entry.getKey(), entry.getValue());
		}

		return prefixed;
	}

	@Override
	public boolean containsKey(String key) {
		return backingConfig.containsKey(prefix + key);
	}

	@Override
	public boolean contains(ConfigOption<?> configOption) {
		return backingConfig.contains(prefixOption(configOption, prefix));
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {
		this.prefix = in.readUTF();
		this.backingConfig.read(in);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeUTF(this.prefix);
		this.backingConfig.write(out);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return this.prefix.hashCode() ^ this.backingConfig.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DelegatingConfiguration) {
			DelegatingConfiguration other = (DelegatingConfiguration) obj;
			return this.prefix.equals(other.prefix) && this.backingConfig.equals(other.backingConfig);
		} else {
			return false;
		}
	}

	// --------------------------------------------------------------------------------------------

	private static <T> ConfigOption<T> prefixOption(ConfigOption<T> option, String prefix) {
		String key = prefix + option.key();

		List<String> deprecatedKeys;
		if (option.hasDeprecatedKeys()) {
			deprecatedKeys = new ArrayList<>();
			for (String dk : option.deprecatedKeys()) {
				deprecatedKeys.add(prefix + dk);
			}
		} else {
			deprecatedKeys = Collections.emptyList();
		}

		String[] deprecated = deprecatedKeys.toArray(new String[deprecatedKeys.size()]);
		return new ConfigOption<>(key,
			option.description(),
			option.defaultValue(),
			deprecated);
	}
}
