package eu.stratosphere.pact.common.plan;

import eu.stratosphere.nephele.configuration.Configuration;

public class PlanConfiguration extends Configuration {

	private static final String NEPHELE_PREFIX = "NEPHELE::";
	
	/**
	 * Returns the Nephele value associated with the given key as a string.
	 * 
	 * @param key
	 *        the key pointing to the associated Nephele value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public String getNepheleString(String key, String defaultValue) {
		return getString(NEPHELE_PREFIX+key, defaultValue);		
	}
	
	/**
	 * Adds the given key/value pair to the Nephele configuration. If either the key
	 * or the value is <code>null</code> the pair is not added.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setString(String key, String value) {
		setString(NEPHELE_PREFIX+key, value);
	}
	
	/**
	 * Returns the Nephele value associated with the given key as an integer.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public int getNepheleInteger(String key, int defaultValue) {
		return getInteger(NEPHELE_PREFIX+key, defaultValue);
	}

	/**
	 * Adds the given key/value pair to the Nephele configuration. If the
	 * key is <code>null</code>, the key/value pair is not added.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setNepheleInteger(String key, int value) {
		setInteger(NEPHELE_PREFIX+key, value);
	}
	
	/**
	 * Returns the Nephele value associated with the given key as a long.
	 * 
	 * @param key
	 *        the key pointing to the associated Nephele value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public long getNepheleLong(String key, long defaultValue) {
		return getLong(NEPHELE_PREFIX+key,defaultValue);
	}

	/**
	 * Adds the given key/value pair to the Nephele configuration. If the
	 * key is <code>null</code>, the key/value pair is not added.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setNepheleLong(String key, long value) {
		setLong(NEPHELE_PREFIX+key, value);
	}

	/**
	 * Returns the Nephele value associated with the given key as a boolean.
	 * 
	 * @param key
	 *        the key pointing to the associated Nephele value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public boolean getNepheleBoolean(String key, boolean defaultValue) {
		return getBoolean(NEPHELE_PREFIX+key, defaultValue);
	}

	/**
	 * Adds the given key/value pair to the Nephele configuration. If key is <code>null</code> the key/value pair is
	 * ignored and not added.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setNepheleBoolean(String key, boolean value) {
		setBoolean(NEPHELE_PREFIX+key, value);
	}
	
	/**
	 * Returns the Nephele value associated with the given key as a float.
	 * 
	 * @param key
	 *        the key pointing to the associated Nephele value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public float getNepheleFloat(String key, float defaultValue) {
		return getFloat(NEPHELE_PREFIX+key, defaultValue);
	}

	/**
	 * Adds the given key/value pair to the Nephele configuration. If key is <code>null</code> the key/value pair is
	 * ignored and not added.
	 * 
	 * @param key
	 *        the key of the key/value pair to be added
	 * @param value
	 *        the value of the key/value pair to be added
	 */
	public void setNepheleFloat(String key, float value) {
		setFloat(NEPHELE_PREFIX+key, value);
	}
	
	/**
	 * Copies all Nephele keys and values into the provided configuration object.
	 * 
	 * @param nepheleConfiguration The configuration into which all Nephele keys and values are copied.
	 */
	public void extractNepheleConfiguration(Configuration nepheleConfiguration) {
		
		for(String key : this.keySet()) {
			if(key.startsWith(NEPHELE_PREFIX)) {
				nepheleConfiguration.setString(key.substring(NEPHELE_PREFIX.length()), this.getString(key, ""));
			}
		}
	}
	
}
