/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import eu.stratosphere.nephele.util.StringUtils;

/**
 * Lightweight configuration object which can store key/value pairs. Configuration objects
 * can be extracted from or integrated into the {@link GlobalConfiguration} object. They can
 * be transported via Nephele's IPC system to distribute configuration data at runtime.
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class Configuration implements KryoSerializable {

	/**
	 * Used to log warnings on error originating from this class.
	 */
	private static final Log LOG = LogFactory.getLog(Configuration.class);

	/**
	 * Stores the concrete key/value pairs of this configuration object.
	 */
	private Map<String, String> confData = new ConcurrentHashMap<String, String>();

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
	 * Constructs a new configuration object and fills it with the entries of the given map.
	 * 
	 * @param entries
	 *        the entries to fill the configuration object with
	 */
	Configuration(final Map<String, String> entries) {
		this.classLoader = this.getClass().getClassLoader();
		this.confData.putAll(entries);
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

	/**
	 * Returns the value associated with the given key as a string.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public String getString(final String key, final String defaultValue) {

		final String retVal = this.confData.get(key);
		if (retVal == null) {
			return defaultValue;
		}

		return retVal;
	}

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
	public <T> Class<T> getClass(final String key, final Class<? extends T> defaultValue, final Class<T> ancestor) {

		final String className = getString(key, null);
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
	public Class<?> getClass(final String key, final Class<?> defaultValue) {
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
	public void setClass(final String key, final Class<?> klazz) {
		setString(key, klazz.getName());
	}

	/**
	 * Adds the given key/value pair to the configuration object. If either the key
	 * or the value is <code>null</code> the pair is not added.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setString(final String key, final String value) {

		if (key == null) {
			throw new NullPointerException("Given key is null");
		}

		if (value == null) {
			throw new NullPointerException("Given value is null");
		}

		this.confData.put(key, value);
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
	public int getInteger(final String key, final int defaultValue) {

		final String str = this.confData.get(key);
		if (str == null) {
			return defaultValue;
		}

		try {
			return Integer.parseInt(str);
		} catch (NumberFormatException nfe) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(StringUtils.stringifyException(nfe));
			}
		}

		return defaultValue;
	}

	/**
	 * Adds the given key/value pair to the configuration object. If the
	 * key is <code>null</code>, the key/value pair is not added.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setInteger(final String key, final int value) {

		if (key == null) {
			throw new NullPointerException("Given key is null");
		}

		this.confData.put(key, Integer.toString(value));
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
	public long getLong(final String key, final long defaultValue) {

		final String str = this.confData.get(key);
		if (str == null) {
			return defaultValue;
		}

		try {
			return Long.parseLong(str);
		} catch (NumberFormatException nfe) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(StringUtils.stringifyException(nfe));
			}
		}

		return defaultValue;
	}

	/**
	 * Adds the given key/value pair to the configuration object. If the
	 * key is <code>null</code>, the key/value pair is not added.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setLong(final String key, final long value) {

		if (key == null) {
			throw new NullPointerException("Given key is null");
		}

		this.confData.put(key, Long.toString(value));
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
	public boolean getBoolean(final String key, final boolean defaultValue) {

		final String str = this.confData.get(key);
		if (str == null) {
			return defaultValue;
		}

		return Boolean.parseBoolean(str);
	}

	/**
	 * Adds the given key/value pair to the configuration object. If key is <code>null</code> the key/value pair is
	 * ignored and not added.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setBoolean(final String key, final boolean value) {

		if (key == null) {
			throw new NullPointerException("Given key is null");
		}

		this.confData.put(key, Boolean.toString(value));
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

		final String str = this.confData.get(key);
		if (str == null) {
			return defaultValue;
		}

		try {
			return Float.parseFloat(str);
		} catch (NumberFormatException nfe) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(StringUtils.stringifyException(nfe));
			}
		}

		return defaultValue;
	}

	/**
	 * Adds the given key/value pair to the configuration object. If key is <code>null</code> the key/value pair is
	 * ignored and not added.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setFloat(final String key, final float value) {

		if (key == null) {
			throw new NullPointerException("Given key is null");
		}

		this.confData.put(key, Float.toString(value));
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
	@Deprecated
	public byte[] getBytes(final String key, final byte[] defaultValue) {

		final String encoded = this.confData.get(key);
		if (encoded == null) {
			return defaultValue;
		}

		return Base64.decodeBase64(encoded.getBytes());
	}

	/**
	 * Adds the given byte array to the configuration object. If key is <code>null</code> then nothing is added.
	 * 
	 * @param key
	 *        The key under which the bytes are added.
	 * @param bytes
	 *        The bytes to be added.
	 */
	@Deprecated
	public void setBytes(final String key, final byte[] bytes) {

		if (key == null) {
			throw new NullPointerException("Given key is null");
		}

		final String encoded = new String(Base64.encodeBase64(bytes));
		this.confData.put(key, encoded);
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
		retVal.addAll(this.confData.keySet());

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

		for (Map.Entry<String, String> entry : other.confData.entrySet()) {
			bld.setLength(pl);
			bld.append(entry.getKey());
			this.confData.put(bld.toString(), entry.getValue());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.confData.hashCode();
		return result;
	}

	/**
	 * {@inheritDoc}
	 */
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Kryo kryo, final Output output) {

		final Set<Map.Entry<String, String>> entries = this.confData.entrySet();
		output.writeInt(entries.size());

		final Iterator<Map.Entry<String, String>> it = entries.iterator();
		while (it.hasNext()) {
			final Map.Entry<String, String> entry = it.next();
			output.writeString(entry.getKey());
			output.writeString(entry.getValue());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final Kryo kryo, final Input input) {

		final int numberOfProperties = input.readInt();

		for (int i = 0; i < numberOfProperties; i++) {
			final String key = input.readString();
			final String value = input.readString();
			this.confData.put(key, value);
		}
	}
}
