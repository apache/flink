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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight configuration object which can store key/value pairs.
 */
@SuppressWarnings("EqualsBetweenInconvertibleTypes")
public class Configuration extends ExecutionConfig.GlobalJobParameters implements IOReadableWritable, java.io.Serializable, Cloneable {

	private static final long serialVersionUID = 1L;
	
	private static final byte TYPE_STRING = 0;
	private static final byte TYPE_INT = 1;
	private static final byte TYPE_LONG = 2;
	private static final byte TYPE_BOOLEAN = 3;
	private static final byte TYPE_FLOAT = 4;
	private static final byte TYPE_DOUBLE = 5;
	private static final byte TYPE_BYTES = 6;
	
	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);
	

	/** Stores the concrete key/value pairs of this configuration object. */
	private final Map<String, Object> confData = new HashMap<String, Object>();
	
	// --------------------------------------------------------------------------------------------
	
	public Configuration() {}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns the class associated with the given key as a string.
	 * 
	 * @param <T> The type of the class to return.

	 * @param key The key pointing to the associated value
	 * @param defaultValue The optional default value returned if no entry exists
	 * @param classLoader The class loader used to resolve the class.
	 * 
	 * @return The value associated with the given key, or the default value, if to entry for the key exists.
	 */
	@SuppressWarnings("unchecked")
	public <T> Class<T> getClass(String key, Class<? extends T> defaultValue, ClassLoader classLoader) throws ClassNotFoundException {
		Object o = getRawValue(key);
		if (o == null) {
			return (Class<T>) defaultValue;
		}
		
		if (o.getClass() == String.class) {
			return (Class<T>) Class.forName((String) o, true, classLoader);
		}
		
		LOG.warn("Configuration cannot evaluate value " + o + " as a class name");
		return (Class<T>) defaultValue;
	}

	/**
	 * Adds the given key/value pair to the configuration object. The class can be retrieved by invoking
	 * {@link #getClass(String, Class, ClassLoader)} if it is in the scope of the class loader on the caller.
	 * 
	 * @param key The key of the pair to be added
	 * @param klazz The value of the pair to be added
	 * @see #getClass(String, Class, ClassLoader)
	 */
	public void setClass(String key, Class<?> klazz) {
		setValueInternal(key, klazz.getName());
	}

	/**
	 * Returns the value associated with the given key as a string.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public String getString(String key, String defaultValue) {
		Object o = getRawValue(key);
		if (o == null) {
			return defaultValue;
		} else {
			return o.toString();
		}
	}
	
	/**
	 * Adds the given key/value pair to the configuration object.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setString(String key, String value) {
		setValueInternal(key, value);
	}

	/**
	 * Returns the value associated with the given key as an integer.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public int getInteger(String key, int defaultValue) {
		Object o = getRawValue(key);
		if (o == null) {
			return defaultValue;
		}
		
		if (o.getClass() == Integer.class) {
			return (Integer) o;
		}
		else if (o.getClass() == Long.class) {
			long value = (Long) o;
			if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
				return (int) value;
			} else {
				LOG.warn("Configuration value {} overflows/underflows the integer type.", value);
				return defaultValue;
			}
		}
		else {
			try {
				return Integer.parseInt(o.toString());
			}
			catch (NumberFormatException e) {
				LOG.warn("Configuration cannot evaluate value {} as an integer number", o);
				return defaultValue;
			}
		}
	}

	/**
	 * Adds the given key/value pair to the configuration object.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setInteger(String key, int value) {
		setValueInternal(key, value);
	}

	/**
	 * Returns the value associated with the given key as a long.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public long getLong(String key, long defaultValue) {
		Object o = getRawValue(key);
		if (o == null) {
			return defaultValue;
		}
		
		if (o.getClass() == Long.class) {
			return (Long) o;
		}
		else if (o.getClass() == Integer.class) {
			return ((Integer) o).longValue();
		}
		else {
			try {
				return Long.parseLong(o.toString());
			}
			catch (NumberFormatException e) {
				LOG.warn("Configuration cannot evaluate value " + o + " as a long integer number");
				return defaultValue;
			}
		}
	}

	/**
	 * Adds the given key/value pair to the configuration object.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setLong(String key, long value) {
		setValueInternal(key, value);
	}

	/**
	 * Returns the value associated with the given key as a boolean.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public boolean getBoolean(String key, boolean defaultValue) {
		Object o = getRawValue(key);
		if (o == null) {
			return defaultValue;
		}
		
		if (o.getClass() == Boolean.class) {
			return (Boolean) o;
		}
		else {
			return Boolean.parseBoolean(o.toString());
		}
	}

	/**
	 * Adds the given key/value pair to the configuration object.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setBoolean(String key, boolean value) {
		setValueInternal(key, value);
	}

	/**
	 * Returns the value associated with the given key as a float.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public float getFloat(String key, float defaultValue) {
		Object o = getRawValue(key);
		if (o == null) {
			return defaultValue;
		}
		
		if (o.getClass() == Float.class) {
			return (Float) o;
		}
		else if (o.getClass() == Double.class) {
			double value = ((Double) o);
			if (value <= Float.MAX_VALUE && value >= Float.MIN_VALUE) {
				return (float) value;
			} else {
				LOG.warn("Configuration value {} overflows/underflows the float type.", value);
				return defaultValue;
			}
		}
		else {
			try {
				return Float.parseFloat(o.toString());
			}
			catch (NumberFormatException e) {
				LOG.warn("Configuration cannot evaluate value {} as a float value", o);
				return defaultValue;
			}
		}
	}

	/**
	 * Adds the given key/value pair to the configuration object.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setFloat(String key, float value) {
		setValueInternal(key, value);
	}
	
	/**
	 * Returns the value associated with the given key as a double.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public double getDouble(String key, double defaultValue) {
		Object o = getRawValue(key);
		if (o == null) {
			return defaultValue;
		}
		
		if (o.getClass() == Double.class) {
			return (Double) o;
		}
		else if (o.getClass() == Float.class) {
			return ((Float) o).doubleValue();
		}
		else {
			try {
				return Double.parseDouble(o.toString());
			}
			catch (NumberFormatException e) {
				LOG.warn("Configuration cannot evaluate value {} as a double value", o);
				return defaultValue;
			}
		}
	}

	/**
	 * Adds the given key/value pair to the configuration object.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setDouble(String key, double value) {
		setValueInternal(key, value);
	}
	
	/**
	 * Returns the value associated with the given key as a byte array.
	 * 
	 * @param key
	 *        The key pointing to the associated value.
	 * @param defaultValue
	 *        The default value which is returned in case there is no value associated with the given key.
	 * @return the (default) value associated with the given key.
	 */
	public byte[] getBytes(String key, byte[] defaultValue) {
		
		Object o = getRawValue(key);
		if (o == null) {
			return defaultValue;
		}
		else if (o.getClass().equals(byte[].class)) {
			return (byte[]) o;
		}
		else {
			LOG.warn("Configuration cannot evaluate value {} as a byte[] value", o);
			return defaultValue;
		}
	}
	
	/**
	 * Adds the given byte array to the configuration object. If key is <code>null</code> then nothing is added.
	 * 
	 * @param key
	 *        The key under which the bytes are added.
	 * @param bytes
	 *        The bytes to be added.
	 */
	public void setBytes(String key, byte[] bytes) {
		setValueInternal(key, bytes);
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns the keys of all key/value pairs stored inside this
	 * configuration object.
	 * 
	 * @return the keys of all key/value pairs stored inside this configuration object
	 */
	public Set<String> keySet() {
		synchronized (this.confData) {
			return new HashSet<String>(this.confData.keySet());
		}
	}

	public void addAll(Configuration other) {
		synchronized (this.confData) {
			synchronized (other.confData) {
				this.confData.putAll(other.confData);
			}
		}
	}
	
	/**
	 * Adds all entries from the given configuration into this configuration. The keys
	 * are prepended with the given prefix.
	 * 
	 * @param other
	 *        The configuration whose entries are added to this configuration.
	 * @param prefix
	 *        The prefix to prepend.
	 */
	public void addAll(Configuration other, String prefix) {
		final StringBuilder bld = new StringBuilder();
		bld.append(prefix);
		final int pl = bld.length();

		synchronized (this.confData) {
			synchronized (other.confData) {
				for (Map.Entry<String, Object> entry : other.confData.entrySet()) {
					bld.setLength(pl);
					bld.append(entry.getKey());
					this.confData.put(bld.toString(), entry.getValue());
				}
			}
		}
	}

	@Override
	public Configuration clone() {
		Configuration config = new Configuration();
		config.addAll(this);

		return config;
	}

	/**
	 * Checks whether there is an entry with the specified key
	 *
	 * @param key key of entry
	 * @return true if the key is stored, false otherwise
	 */
	public boolean containsKey(String key){
		synchronized (this.confData){
			return this.confData.containsKey(key);
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public Map<String, String> toMap() {
		synchronized (this.confData){
			Map<String, String> ret = new HashMap<String, String>(this.confData.size());
			for(Map.Entry<String, Object> entry : confData.entrySet()) {
				ret.put(entry.getKey(), entry.getValue().toString());
			}
			return ret;
		}
	}


	// --------------------------------------------------------------------------------------------
	
	private <T> void setValueInternal(String key, T value) {
		if (key == null) {
			throw new NullPointerException("Key must not be null.");
		}
		if (value == null) {
			throw new NullPointerException("Value must not be null.");
		}
		
		synchronized (this.confData) {
			this.confData.put(key, value);
		}
	}
	
	private Object getRawValue(String key) {
		if (key == null) {
			throw new NullPointerException("Key must not be null.");
		}
		
		synchronized (this.confData) {
			return this.confData.get(key);
		}
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {
		synchronized (this.confData) {
			final int numberOfProperties = in.readInt();

			for (int i = 0; i < numberOfProperties; i++) {
				String key = StringValue.readString(in);
				Object value;
				
				byte type = in.readByte();
				switch (type) {
					case TYPE_STRING:
						value = StringValue.readString(in);
						break;
					case TYPE_INT:
						value = in.readInt();
						break;
					case TYPE_LONG:
						value = in.readLong();
						break;
					case TYPE_FLOAT:
						value = in.readFloat();
						break;
					case TYPE_DOUBLE:
						value = in.readDouble();
						break;
					case TYPE_BOOLEAN:
						value = in.readBoolean();
						break;
					case TYPE_BYTES:
						byte[] bytes = new byte[in.readInt()];
						in.readFully(bytes);
						value = bytes;
						break;
					default:
						throw new IOException("Unrecognized type: " + type);
				}
				
				this.confData.put(key, value);
			}
		}
	}

	@Override
	public void write(final DataOutputView out) throws IOException {
		synchronized (this.confData) {
			out.writeInt(this.confData.size());
			
			for (Map.Entry<String, Object> entry : this.confData.entrySet()) {
				String key = entry.getKey();
				Object val = entry.getValue();
						
				StringValue.writeString(key, out);
				Class<?> clazz = val.getClass();
				
				if (clazz == String.class) {
					out.write(TYPE_STRING);
					StringValue.writeString((String) val, out);
				}
				else if (clazz == Integer.class) {
					out.write(TYPE_INT);
					out.writeInt((Integer) val);
				}
				else if (clazz == Long.class) {
					out.write(TYPE_LONG);
					out.writeLong((Long) val);
				}
				else if (clazz == Float.class) {
					out.write(TYPE_FLOAT);
					out.writeFloat((Float) val);
				}
				else if (clazz == Double.class) {
					out.write(TYPE_DOUBLE);
					out.writeDouble((Double) val);
				}
				else if (clazz == byte[].class) {
					out.write(TYPE_BYTES);
					byte[] bytes = (byte[]) val;
					out.writeInt(bytes.length);
					out.write(bytes);
				}
				else if (clazz == Boolean.class) {
					out.write(TYPE_BOOLEAN);
					out.writeBoolean((Boolean) val);
				}
				else {
					throw new IllegalArgumentException("Unrecognized type");
				}
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		int hash = 0;
		for (String s : this.confData.keySet()) {
			hash ^= s.hashCode();
		}
		return hash;
	}

	@SuppressWarnings("EqualsBetweenInconvertibleTypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		else if (obj instanceof Configuration) {
			Map<String, Object> otherConf = ((Configuration) obj).confData;
			
			for (Map.Entry<String, Object> e : this.confData.entrySet()) {
				Object thisVal = e.getValue();
				Object otherVal = otherConf.get(e.getKey());
				
				if (!thisVal.getClass().equals(byte[].class)) {
					if (!thisVal.equals(otherVal)) {
						return false;
					}
				} else if (otherVal.getClass().equals(byte[].class)) {
					if (!Arrays.equals((byte[]) thisVal, (byte[]) otherVal)) {
						return false;
					}
				} else {
					return false;
				}
			}
			
			return true;
		}
		else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return this.confData.toString();
	}
}
