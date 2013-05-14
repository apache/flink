/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.types.StringRecord;

/**
 * Lightweight configuration object which can store key/value pairs. Configuration objects
 * can be extracted from or integrated into the {@link GlobalConfiguration} object. They can
 * be transported via Nephele's IPC system to distribute configuration data at runtime.
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class Configuration implements IOReadableWritable {

	/**
	 * Stores the concrete key/value pairs of this configuration object.
	 */
	private Map<String, String> confData = new HashMap<String, String>();

	/**
	 * The class loader to be used for the <code>getClass</code> method.
	 */
	private final ClassLoader classLoader;

	/**
	 * Constructs a new configuration object.
	 */
	public Configuration() {
		this.classLoader = this.getClass().getClassLoader();
	}

	/**
	 * Constructs a new configuration object.
	 * 
	 * @param classLoader
	 *        the class loader to be use for the <code>getClass</code> method
	 */
	public Configuration(final ClassLoader classLoader) {
		this.classLoader = classLoader;
	}
	
	
	public ClassLoader getClassLoader() {
		return this.classLoader;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the class associated with the given key as a string.
	 * 
	 * @param <T>
	 *        the ancestor of both the default value and the potential value
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the optional default value returned if no entry exists
	 * @param ancestor
	 *        the ancestor of both the default value and the potential value
	 * @return the (default) value associated with the given key
	 * @throws IllegalStateException
	 *         if the class identified by the associated value cannot be resolved
	 * @see #setClass(String, Class)
	 */
	@SuppressWarnings("unchecked")
	public <T> Class<T> getClass(String key, Class<? extends T> defaultValue, Class<T> ancestor) {
		String className = getStringInternal(key);
		if (className == null) {
			return (Class<T>) defaultValue;
		}

		try {
			return (Class<T>) Class.forName(className, true, this.classLoader);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Returns the class associated with the given key as a string.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 * @throws IllegalStateException
	 *         if the class identified by the associated value cannot be resolved
	 * @see #setClass(String, Class)
	 */
	public Class<?> getClass(String key, Class<?> defaultValue) {
		return getClass(key, defaultValue, Object.class);
	}

	/**
	 * Adds the given key/value pair to the configuration object. The class can be retrieved by invoking
	 * {@link #getClass(String, Class, Class)} if it is in the scope of the class loader on the caller.
	 * 
	 * @param key
	 *        the key of the pair to be added
	 * @param klazz
	 *        the value of the pair to be added
	 * @see #getClass(String, Class)
	 * @see #getClass(String, Class, Class)
	 */
	public void setClass(String key, Class<?> klazz) {
		setStringInternal(key, klazz.getName());
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
		String val = getStringInternal(key);
		return val == null ? defaultValue : val;
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
		setStringInternal(key, value);
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
		String val = getStringInternal(key);
		if (val == null) {
			return defaultValue;
		} else {
			return Integer.parseInt(val);
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
		setStringInternal(key, Integer.toString(value));
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
		String val = getStringInternal(key);
		if (val == null) {
			return defaultValue;
		} else {
			return Long.parseLong(val);
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
		setStringInternal(key, Long.toString(value));
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
		String val = getStringInternal(key);
		if (val == null) {
			return defaultValue;
		} else {
			return Boolean.parseBoolean(val);
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
	public void setBoolean(final String key, final boolean value) {
		setStringInternal(key, Boolean.toString(value));
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
	public float getFloat(final String key, final float defaultValue) {
		String val = getStringInternal(key);
		if (val == null) {
			return defaultValue;
		} else {
			return Float.parseFloat(val);
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
		setStringInternal(key, Float.toString(value));
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
		String val = getStringInternal(key);
		if (val == null) {
			return defaultValue;
		} else {
			return Double.parseDouble(val);
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
		setStringInternal(key, Double.toString(value));
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
		String encoded = getStringInternal(key);
		if (encoded == null) {
			return defaultValue;
		} else {
			return Base64.decodeBase64(encoded.getBytes());
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
		String encoded = new String(Base64.encodeBase64(bytes));
		setStringInternal(key, encoded);
	}

	/**
	 * Returns the keys of all key/value pairs stored inside this
	 * configuration object.
	 * 
	 * @return the keys of all key/value pairs stored inside this configuration object
	 */
	public Set<String> keySet() {

		// Copy key set, so return value is independent from the object's internal data structure
		final Set<String> retVal = new HashSet<String>();

		synchronized (this.confData) {

			final Iterator<String> it = this.confData.keySet().iterator();
			while (it.hasNext()) {
				retVal.add(it.next());
			}
		}

		return retVal;
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
				for (Map.Entry<String, String> entry : other.confData.entrySet()) {
					bld.setLength(pl);
					bld.append(entry.getKey());
					this.confData.put(bld.toString(), entry.getValue());
				}
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private String getStringInternal(String key) {
		if (key == null)
			throw new NullPointerException("Key mus not be null.");
		
		synchronized (this.confData) {
			return this.confData.get(key);
		}
	}
	
	private void setStringInternal(String key, String value) {
		if (key == null)
			throw new NullPointerException("Key mus not be null.");
		if (value == null)
			throw new NullPointerException("Value mus not be null.");
			
		
		synchronized (this.confData) {
			this.confData.put(key, value);
		}
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		synchronized (this.confData) {

			final int numberOfProperties = in.readInt();

			for (int i = 0; i < numberOfProperties; i++) {
				final String key = StringRecord.readString(in);
				final String value = StringRecord.readString(in);
				this.confData.put(key, value);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		synchronized (this.confData) {

			out.writeInt(this.confData.size());

			final Iterator<String> it = this.confData.keySet().iterator();
			while (it.hasNext()) {
				final String key = it.next();
				final String value = this.confData.get(key);
				StringRecord.writeString(out, key);
				StringRecord.writeString(out, value);
			}
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + confData.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final Configuration other = (Configuration) obj;
		return confData.equals(other.confData);
	}
}