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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
	 * Used to log warnings on error originating from this class.
	 */
	private static final Log LOG = LogFactory.getLog(Configuration.class);

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

		synchronized (this.confData) {

			final String retVal = this.confData.get(key);
			if (retVal == null) {
				return defaultValue;
			}

			return retVal;
		}
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

		if (key == null || value == null) {
			// TODO: should probably throw an NullPointerException
			LOG.warn("Key/value pair " + key + ", " + value + " not added to configuration");
			return;
		}

		synchronized (this.confData) {
			this.confData.put(key, value);
		}
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

		int retVal = defaultValue;

		try {
			synchronized (this.confData) {

				if (this.confData.containsKey(key)) {
					retVal = Integer.parseInt(this.confData.get(key));
				}
			}
		} catch (NumberFormatException e) {
			LOG.debug(e);
		}

		return retVal;
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
			LOG.warn("Cannot set integer: Given key is null!");
			return;
		}

		synchronized (this.confData) {
			this.confData.put(key, Integer.toString(value));
		}
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
		try {
			synchronized (this.confData) {
				String val = this.confData.get(key);
				if (val != null) {
					return Long.parseLong(val);
				}
			}
		} catch (NumberFormatException e) {
			LOG.debug(e);
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
			LOG.warn("Cannot set integer: Given key is null!");
			return;
		}

		synchronized (this.confData) {
			this.confData.put(key, Long.toString(value));
		}
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

		boolean retVal = defaultValue;

		synchronized (this.confData) {

			if (this.confData.containsKey(key)) {
				retVal = Boolean.parseBoolean(this.confData.get(key));
			}
		}

		return retVal;
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
			LOG.warn("Cannot set boolean: Given key is null!");
			return;
		}

		synchronized (this.confData) {
			this.confData.put(key, Boolean.toString(value));
		}
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
		synchronized (this.confData) {
			String val = this.confData.get(key);
			return val == null ? defaultValue : Float.parseFloat(val);
		}
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
			LOG.warn("Cannot set boolean: Given key is null!");
			return;
		}

		synchronized (this.confData) {
			this.confData.put(key, Float.toString(value));
		}
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
	public byte[] getBytes(final String key, final byte[] defaultValue) {
		final String encoded;
		synchronized (this.confData) {
			encoded = this.confData.get(key);
		}
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
	public void setBytes(final String key, final byte[] bytes) {
		if (key == null) {
			LOG.warn("Cannot set boolean: Given key is null!");
			return;
		}

		final String encoded = new String(Base64.encodeBase64(bytes));
		synchronized (this.confData) {
			this.confData.put(key, encoded);
		}
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