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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple adapter between {@link ReadableConfig} and {@link Configuration}.
 * It is used to bridge some of the old public interfaces that work with {@link Configuration} even though they
 * should actually work with {@link ReadableConfig}.
 */
@Internal
public class ReadableConfigToConfigurationAdapter extends Configuration {
	private final ReadableConfig backingConfig;

	public ReadableConfigToConfigurationAdapter(ReadableConfig backingConfig) {
		this.backingConfig = checkNotNull(backingConfig);
	}

	@Override
	public String getString(ConfigOption<String> configOption) {
		return backingConfig.get(configOption);
	}

	@Override
	public String getString(ConfigOption<String> configOption, String overrideDefault) {
		return backingConfig.getOptional(configOption).orElse(overrideDefault);
	}

	@Override
	public int getInteger(ConfigOption<Integer> configOption) {
		return backingConfig.get(configOption);
	}

	@Override
	public int getInteger(ConfigOption<Integer> configOption, int overrideDefault) {
		return backingConfig.getOptional(configOption).orElse(overrideDefault);
	}

	@Override
	public long getLong(ConfigOption<Long> configOption) {
		return backingConfig.get(configOption);
	}

	@Override
	public long getLong(ConfigOption<Long> configOption, long overrideDefault) {
		return backingConfig.getOptional(configOption).orElse(overrideDefault);
	}

	@Override
	public boolean getBoolean(ConfigOption<Boolean> configOption) {
		return backingConfig.get(configOption);
	}

	@Override
	public boolean getBoolean(ConfigOption<Boolean> configOption, boolean overrideDefault) {
		return backingConfig.getOptional(configOption).orElse(overrideDefault);
	}

	@Override
	public float getFloat(ConfigOption<Float> configOption) {
		return backingConfig.get(configOption);
	}

	@Override
	public float getFloat(ConfigOption<Float> configOption, float overrideDefault) {
		return backingConfig.getOptional(configOption).orElse(overrideDefault);
	}

	@Override
	public double getDouble(ConfigOption<Double> configOption) {
		return backingConfig.get(configOption);
	}

	@Override
	public double getDouble(ConfigOption<Double> configOption, double overrideDefault) {
		return backingConfig.getOptional(configOption).orElse(overrideDefault);
	}

	@Override
	public <T> T get(ConfigOption<T> option) {
		return backingConfig.get(option);
	}

	@Override
	public <T> Optional<T> getOptional(ConfigOption<T> option) {
		return backingConfig.getOptional(option);
	}

	@Override
	public boolean contains(ConfigOption<?> configOption) {
		return this.backingConfig.getOptional(configOption).isPresent();
	}

	@Override
	public int hashCode() {
		return backingConfig.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return backingConfig.equals(obj);
	}

	@Override
	public String toString() {
		return backingConfig.toString();
	}

	/*
		Modifying methods
	*/

	@Override
	public void setClass(String key, Class<?> klazz) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void setString(String key, String value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void setString(ConfigOption<String> key, String value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void setInteger(String key, int value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void setInteger(ConfigOption<Integer> key, int value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void setLong(String key, long value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void setLong(ConfigOption<Long> key, long value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void setBoolean(String key, boolean value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void setBoolean(ConfigOption<Boolean> key, boolean value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void setFloat(String key, float value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void setFloat(ConfigOption<Float> key, float value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void setDouble(String key, double value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void setDouble(ConfigOption<Double> key, double value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void setBytes(String key, byte[] bytes) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void addAllToProperties(Properties props) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void addAll(Configuration other) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public void addAll(Configuration other, String prefix) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public <T> Configuration set(ConfigOption<T> option, T value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	<T> void setValueInternal(String key, T value) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	@Override
	public <T> boolean removeConfig(ConfigOption<T> configOption) {
		throw new UnsupportedOperationException("The configuration is read only");
	}

	/*
	 * Other unsupported options.
	 */

	@Override
	public byte[] getBytes(String key, byte[] defaultValue) {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public String getValue(ConfigOption<?> configOption) {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public <T extends Enum<T>> T getEnum(
		Class<T> enumClass,
		ConfigOption<String> configOption) {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public Configuration clone() {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public boolean containsKey(String key) {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public double getDouble(String key, double defaultValue) {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public <T> Class<T> getClass(
		String key,
		Class<? extends T> defaultValue,
		ClassLoader classLoader) throws ClassNotFoundException {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public String getString(String key, String defaultValue) {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public int getInteger(String key, int defaultValue) {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public long getLong(String key, long defaultValue) {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public boolean getBoolean(String key, boolean defaultValue) {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public float getFloat(String key, float defaultValue) {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public Set<String> keySet() {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public Map<String, String> toMap() {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public void read(DataInputView in) throws IOException {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		throw new UnsupportedOperationException("The adapter does not support this method");
	}
}
