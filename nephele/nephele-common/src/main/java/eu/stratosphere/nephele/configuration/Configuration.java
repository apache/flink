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
	 * Returns the value associated with the given key as a string.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public String getString(String key, String defaultValue) {

		synchronized (this.confData) {

			if (!this.confData.containsKey(key)) {
				return defaultValue;
			}

			return this.confData.get(key);
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
	public <T> Class<T> getClass(String key, Class<? extends T> defaultValue, Class<T> ancestor) {
		String className = getString(key, null);
		if (className == null)
			return (Class<T>) defaultValue;
		try {
			return (Class<T>) Class.forName(className);
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
	public void setString(String key, String value) {

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
	public int getInteger(String key, int defaultValue) {

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
	public void setInteger(String key, int value) {

		if (key == null) {
			LOG.warn("Cannot set integer: Given key is null!");
			return;
		}

		synchronized (this.confData) {
			this.confData.put(key, Integer.toString(value));
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
	public boolean getBoolean(String key, boolean defaultValue) {

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
	public void setBoolean(String key, boolean value) {

		if (key == null) {
			LOG.warn("Cannot set boolean: Given key is null!");
			return;
		}

		synchronized (this.confData) {
			this.confData.put(key, Boolean.toString(value));
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
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {

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
	public void write(DataOutput out) throws IOException {

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
	public boolean equals(Object obj) {

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
