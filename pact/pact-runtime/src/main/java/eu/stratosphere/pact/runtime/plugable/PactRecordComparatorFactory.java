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

package eu.stratosphere.pact.runtime.plugable;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.task.util.CorruptConfigurationException;

/**
 * A factory for a {@link TypeComparator} for {@link PactRecord}. The comparator uses a subset of
 * the fields for the comparison. That subset of fields (positions and types) is read from the
 * supplied configuration.
 *
 * @author Stephan Ewen
 */
public class PactRecordComparatorFactory implements TypeComparatorFactory<PactRecord>
{
	private static final String NUM_KEYS = "pact.input.numkeys";
	
	private static final String KEY_POS_PREFIX = "pact.input.keypos.";
	
	private static final String KEY_CLASS_PREFIX = "pact.input.keyclass.";
	
	private static final String NUM_SS_KEYS = "pact.input.numsskeys";
	
	private static final String SS_KEY_POS_PREFIX = "pact.input.sskeypos.";
	
	private static final String SS_KEY_CLASS_PREFIX = "pact.input.sskeyclass.";
	
	private static final PactRecordComparatorFactory INSTANCE = new PactRecordComparatorFactory();
	
	/**
	 * Gets an instance of the comparator factory. The instance is shared, since the factory is a
	 * stateless class. 
	 * 
	 * @return An instance of the comparator factory.
	 */
	public static final PactRecordComparatorFactory get() {
		return INSTANCE;
	}
	
	// --------------------------------------------------------------------------------------------

	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeComparatorFactory#createComparator(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public PactRecordComparator createComparator(Configuration config, String keyPrefix, ClassLoader cl)
	throws ClassNotFoundException
	{
		// figure out how many key fields there are
		final int numKeyFields = config.getInteger(keyPrefix + NUM_KEYS, -1);
		if (numKeyFields < 0) {
			throw new CorruptConfigurationException("The number of keys for the comparator with config prefix '" +
					keyPrefix + "' is invalid: " + numKeyFields);
		}
		
		final int[] positions = new int[numKeyFields];
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] types = new Class[numKeyFields];
		
		// read the individual key positions and types
		for (int i = 0; i < numKeyFields; i++) {
			// next key position
			final int p = config.getInteger(keyPrefix + KEY_POS_PREFIX + i, -1);
			if (p >= 0) {
				positions[i] = p;
			} else {
				throw new CorruptConfigurationException("Contained invalid position for key no positions for keys."); 
			}
			
			// next key type
			final String name = config.getString(keyPrefix + KEY_CLASS_PREFIX + i, null);
			if (name != null) {
				types[i] = Class.forName(name, true, cl).asSubclass(Key.class);
			} else {
				throw new CorruptConfigurationException("The key type (" + i + 
					") for the comparator with config prefix '" + keyPrefix + "' is null"); 
			}
		}
		
		return new PactRecordComparator(positions, types);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeComparatorFactory#createSecondaySortComparator(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public TypeComparator<PactRecord> createSecondarySortComparator(Configuration config, String keyPrefix, ClassLoader cl)
	throws ClassNotFoundException
	{
		final int numSSKeyFields = config.getInteger(keyPrefix + NUM_SS_KEYS, -1);
		if (numSSKeyFields == -1) {
			// no secondary sort comparator set
			return null;
		}
		
		final int numKeyFields = config.getInteger(keyPrefix + NUM_KEYS, -1);
		
		if (numKeyFields < 0) {
			throw new CorruptConfigurationException("The number of keys for the comparator with config prefix '" +
					keyPrefix + "' is invalid: " + numKeyFields);
		}
		if (numSSKeyFields < 0) {
			throw new CorruptConfigurationException("The number of secondary sort keys for the comparator with config prefix '" +
					keyPrefix + "' is invalid: " + numSSKeyFields);
		}
		
		final int[] positions = new int[numKeyFields + numSSKeyFields];
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] types = new Class[numKeyFields + numSSKeyFields];
		
		// read the individual key positions and types
		for (int i = 0; i < numKeyFields; i++) {
			// next key position
			final int p = config.getInteger(keyPrefix + KEY_POS_PREFIX + i, -1);
			if (p >= 0) {
				positions[i] = p;
			} else {
				throw new CorruptConfigurationException("Contained invalid position for key no positions for keys."); 
			}
			
			// next key type
			final String name = config.getString(keyPrefix + KEY_CLASS_PREFIX + i, null);
			if (name != null) {
				types[i] = Class.forName(name, true, cl).asSubclass(Key.class);
			} else {
				throw new CorruptConfigurationException("The key type (" + i + 
					") for the comparator with config prefix '" + keyPrefix + "' is null"); 
			}
		}
		
		// read the individual key positions and types
		for (int i = 0; i < numSSKeyFields; i++) {
			// next key position
			final int p = config.getInteger(keyPrefix + SS_KEY_POS_PREFIX + i, -1);
			if (p >= 0) {
				positions[numKeyFields + i] = p;
			} else {
				throw new CorruptConfigurationException("Contained invalid position for key no positions for keys."); 
			}
			
			// next key type
			final String name = config.getString(keyPrefix + SS_KEY_CLASS_PREFIX + i, null);
			if (name != null) {
				types[numKeyFields + i] = Class.forName(name, true, cl).asSubclass(Key.class);
			} else {
				throw new CorruptConfigurationException("The key type (" + i + 
					") for the comparator with config prefix '" + keyPrefix + "' is null"); 
			}
		}
		
		return new PactRecordComparator(positions, types);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeComparatorFactory#getDataType()
	 */
	@Override
	public Class<PactRecord> getDataType() {
		return PactRecord.class;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static void writeComparatorSetupToConfig(Configuration config, String configKeyPrefix, 
			int[] keyPositions, Class<? extends Key>[] keyTypes,
			int[] secondarySortKeyPositions, Class<? extends Key>[] secondarySortKeyTypes)
	{
		// sanity checks
		if (keyPositions.length != keyTypes.length) {
			throw new IllegalArgumentException("The number of key positions and the number of key type classes must be identical.");
		}
		for (int i = 0; i < keyPositions.length; i++) {
			if (keyPositions[i] < 0) {
				throw new IllegalArgumentException("The key position " + i + " is invalid: " + keyPositions[i]);
			}
			if (keyTypes[i] == null || !Key.class.isAssignableFrom(keyTypes[i])) {
				throw new IllegalArgumentException("The key type " + i + " is null or not implenting the interface " + 
					Key.class.getName() + ".");
			}
		}
		
		// write the config
		config.setInteger(configKeyPrefix + NUM_KEYS, keyPositions.length);
		for (int i = 0; i < keyPositions.length; i++) {
			config.setInteger(configKeyPrefix + KEY_POS_PREFIX + i, keyPositions[i]);
			config.setString(configKeyPrefix + KEY_CLASS_PREFIX + i, keyTypes[i].getName());
		}
		
		// now the same for secondary sort keys
		// sanity checks
		if (secondarySortKeyPositions.length != secondarySortKeyTypes.length) {
			throw new IllegalArgumentException("The number of key positions and the number of key type classes must be identical.");
		}
		for (int i = 0; i < secondarySortKeyPositions.length; i++) {
			if (secondarySortKeyPositions[i] < 0) {
				throw new IllegalArgumentException("The key position " + i + " is invalid: " + secondarySortKeyPositions[i]);
			}
			if (secondarySortKeyTypes[i] == null || !Key.class.isAssignableFrom(secondarySortKeyTypes[i])) {
				throw new IllegalArgumentException("The key type " + i + " is null or not implenting the interface " + 
					Key.class.getName() + ".");
			}
		}
		
		// write the config
		config.setInteger(configKeyPrefix + NUM_SS_KEYS, secondarySortKeyPositions.length);
		for (int i = 0; i < secondarySortKeyPositions.length; i++) {
			config.setInteger(configKeyPrefix + SS_KEY_POS_PREFIX + i, secondarySortKeyPositions[i]);
			config.setString(configKeyPrefix + SS_KEY_CLASS_PREFIX + i, secondarySortKeyTypes[i].getName());
		}
	}
	
	public static void writeComparatorSetupToConfig(Configuration config, String configKeyPrefix, 
			int[] keyPositions, Class<? extends Key>[] keyTypes)
	{
		// sanity checks
		if (keyPositions.length != keyTypes.length) {
			throw new IllegalArgumentException("The number of key positions and the number of key type classes must be identical.");
		}
		for (int i = 0; i < keyPositions.length; i++) {
			if (keyPositions[i] < 0) {
				throw new IllegalArgumentException("The key position " + i + " is invalid: " + keyPositions[i]);
			}
			if (keyTypes[i] == null || !Key.class.isAssignableFrom(keyTypes[i])) {
				throw new IllegalArgumentException("The key type " + i + " is null or not implenting the interface " + 
					Key.class.getName() + ".");
			}
		}
		
		// write the config
		config.setInteger(configKeyPrefix + NUM_KEYS, keyPositions.length);
		for (int i = 0; i < keyPositions.length; i++) {
			config.setInteger(configKeyPrefix + KEY_POS_PREFIX + i, keyPositions[i]);
			config.setString(configKeyPrefix + KEY_CLASS_PREFIX + i, keyTypes[i].getName());
		}
		
		// write the config
		config.setInteger(configKeyPrefix + NUM_SS_KEYS, 0);
	}
}
