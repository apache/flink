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

import java.util.Arrays;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
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
	
	private static final String KEY_SORT_DIRECTION_PREFIX = "pact.input.key_direction.";
	
	private static final String NUM_SS_KEYS = "pact.input.numsskeys";
	
//	private static final String SS_KEY_POS_PREFIX = "pact.input.sskeypos.";
//	
//	private static final String SS_KEY_CLASS_PREFIX = "pact.input.sskeyclass.";
//	
//	private static final String SS_KEY_SORT_DIRECTION_PREFIX = "pact.input.sskey_direction.";
	
	// --------------------------------------------------------------------------------------------
	
	private int[] positions;
	
	private Class<? extends Key>[] types;
	
	private boolean[] sortDirections;
	
	// --------------------------------------------------------------------------------------------
	
	public PactRecordComparatorFactory() {
		// do nothing, allow to be configured via config
	}
	
	public PactRecordComparatorFactory(int[] positions, Class<? extends Key>[] types) {
		this(positions, types, null);
	}
	
	public PactRecordComparatorFactory(int[] positions, Class<? extends Key>[] types, boolean[] sortDirections) {
		if (positions == null || types == null)
			throw new NullPointerException();
		if (positions.length != types.length)
			throw new IllegalArgumentException();
		
		this.positions = positions;
		this.types = types;
		
		if (sortDirections == null) {
			this.sortDirections = new boolean[positions.length];
			Arrays.fill(this.sortDirections, true);
		} if (sortDirections.length != positions.length) {
			throw new IllegalArgumentException();
		} else {
			this.sortDirections = sortDirections;
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeComparatorFactory#writeParametersToConfig(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void writeParametersToConfig(Configuration config) {
		for (int i = 0; i < this.positions.length; i++) {
			if (this.positions[i] < 0) {
				throw new IllegalArgumentException("The key position " + i + " is invalid: " + this.positions[i]);
			}
			if (this.types[i] == null || !Key.class.isAssignableFrom(this.types[i])) {
				throw new IllegalArgumentException("The key type " + i + " is null or not implenting the interface " + 
					Key.class.getName() + ".");
			}
		}
		
		// write the config
		config.setInteger(NUM_KEYS, this.positions.length);
		for (int i = 0; i < this.positions.length; i++) {
			config.setInteger(KEY_POS_PREFIX + i, this.positions[i]);
			config.setString(KEY_CLASS_PREFIX + i, this.types[i].getName());
			config.setBoolean(KEY_SORT_DIRECTION_PREFIX + i, this.sortDirections[i]);
		}
		
		// write the config
		config.setInteger(NUM_SS_KEYS, 0);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeComparatorFactory#readParametersFromConfig(eu.stratosphere.nephele.configuration.Configuration, java.lang.ClassLoader)
	 */
	@Override
	public void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException
	{
		// figure out how many key fields there are
		final int numKeyFields = config.getInteger(NUM_KEYS, -1);
		if (numKeyFields < 0) {
			throw new CorruptConfigurationException("The number of keys for the comparator is invalid: " + numKeyFields);
		}
		
		final int[] positions = new int[numKeyFields];
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] types = new Class[numKeyFields];
		final boolean[] direction = new boolean[numKeyFields];
		
		// read the individual key positions and types
		for (int i = 0; i < numKeyFields; i++) {
			// next key position
			final int p = config.getInteger(KEY_POS_PREFIX + i, -1);
			if (p >= 0) {
				positions[i] = p;
			} else {
				throw new CorruptConfigurationException("Contained invalid position for key no positions for keys."); 
			}
			
			// next key type
			final String name = config.getString(KEY_CLASS_PREFIX + i, null);
			if (name != null) {
				types[i] = Class.forName(name, true, cl).asSubclass(Key.class);
			} else {
				throw new CorruptConfigurationException("The key type (" + i + 
					") for the comparator is null"); 
			}
			
			// next key sort direction
			direction[i] = config.getBoolean(KEY_SORT_DIRECTION_PREFIX + i, true);
		}
		
		this.positions = positions;
		this.types = types;
		this.sortDirections = direction;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeComparatorFactory#getDataType()
	 */
	@Override
	public Class<PactRecord> getDataType() {
		return PactRecord.class;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeComparatorFactory#createComparator()
	 */
	@Override
	public TypeComparator<PactRecord> createComparator() {
		return new PactRecordComparator(this.positions, this.types, this.sortDirections);
	}
	
//	/* (non-Javadoc)
//	 * @see eu.stratosphere.pact.common.generic.types.TypeComparatorFactory#createSecondarySortComparator(eu.stratosphere.nephele.configuration.Configuration, java.lang.ClassLoader)
//	 */
//	@Override
//	public TypeComparator<PactRecord> createSecondarySortComparator(Configuration config, ClassLoader cl)
//	throws ClassNotFoundException
//	{
//		final int numSSKeyFields = config.getInteger(NUM_SS_KEYS, -1);
//		if (numSSKeyFields == -1) {
//			// no secondary sort comparator set
//			return null;
//		}
//		
//		final int numKeyFields = config.getInteger(NUM_KEYS, -1);
//		
//		if (numKeyFields < 0) {
//			throw new CorruptConfigurationException("The number of keys for the comparator is invalid: " + numKeyFields);
//		}
//		if (numSSKeyFields < 0) {
//			throw new CorruptConfigurationException("The number of secondary sort keys for the comparator is invalid: " + numSSKeyFields);
//		}
//		
//		final int[] positions = new int[numKeyFields + numSSKeyFields];
//		@SuppressWarnings("unchecked")
//		final Class<? extends Key>[] types = new Class[numKeyFields + numSSKeyFields];
//		final boolean[] direction = new boolean[numKeyFields + numSSKeyFields];
//		
//		// read the individual key positions and types
//		for (int i = 0; i < numKeyFields; i++) {
//			// next key position
//			final int p = config.getInteger(KEY_POS_PREFIX + i, -1);
//			if (p >= 0) {
//				positions[i] = p;
//			} else {
//				throw new CorruptConfigurationException("Contained invalid position for key no positions for keys."); 
//			}
//			
//			// next key type
//			final String name = config.getString(KEY_CLASS_PREFIX + i, null);
//			if (name != null) {
//				types[i] = Class.forName(name, true, cl).asSubclass(Key.class);
//			} else {
//				throw new CorruptConfigurationException("The key type (" + i + ") for the comparator is null"); 
//			}
//			
//			// next key sort direction
//			direction[i] = config.getBoolean(KEY_SORT_DIRECTION_PREFIX + i, true);
//		}
//		
//		// read the individual key positions and types
//		for (int i = 0; i < numSSKeyFields; i++) {
//			// next key position
//			final int p = config.getInteger(SS_KEY_POS_PREFIX + i, -1);
//			if (p >= 0) {
//				positions[numKeyFields + i] = p;
//			} else {
//				throw new CorruptConfigurationException("Contained invalid position for key no positions for keys."); 
//			}
//			
//			// next key type
//			final String name = config.getString(SS_KEY_CLASS_PREFIX + i, null);
//			if (name != null) {
//				types[numKeyFields + i] = Class.forName(name, true, cl).asSubclass(Key.class);
//			} else {
//				throw new CorruptConfigurationException("The key type (" + i + ") for the comparator is null"); 
//			}
//			// next key sort direction
//			direction[numKeyFields + i] = config.getBoolean(SS_KEY_SORT_DIRECTION_PREFIX + i, true);
//		}
//		
//		return new PactRecordComparator(positions, types, direction);
//	}
	
	// --------------------------------------------------------------------------------------------
	
//	public static void writeComparatorSetupToConfig(Configuration config,
//			int[] keyPositions, Class<? extends Key>[] keyTypes, boolean[] directions,
//			int[] secondarySortKeyPositions, Class<? extends Key>[] secondarySortKeyTypes, boolean[] secondarySortDirections)
//	{
//		// sanity checks
//		if (keyPositions.length != keyTypes.length || directions.length != keyTypes.length) {
//			throw new IllegalArgumentException("The number of key positions, key type classes, and key sort directions must be identical.");
//		}
//		for (int i = 0; i < keyPositions.length; i++) {
//			if (keyPositions[i] < 0) {
//				throw new IllegalArgumentException("The key position " + i + " is invalid: " + keyPositions[i]);
//			}
//			if (keyTypes[i] == null || !Key.class.isAssignableFrom(keyTypes[i])) {
//				throw new IllegalArgumentException("The key type " + i + " is null or not implenting the interface " + 
//					Key.class.getName() + ".");
//			}
//		}
//		
//		// write the config
//		config.setInteger(NUM_KEYS, keyPositions.length);
//		for (int i = 0; i < keyPositions.length; i++) {
//			config.setInteger(KEY_POS_PREFIX + i, keyPositions[i]);
//			config.setString(KEY_CLASS_PREFIX + i, keyTypes[i].getName());
//			config.setBoolean(KEY_SORT_DIRECTION_PREFIX, directions[i]);
//		}
//		
//		// now the same for secondary sort keys
//		// sanity checks
//		if (secondarySortKeyPositions.length != secondarySortKeyTypes.length || secondarySortDirections.length != secondarySortKeyTypes.length) {
//			throw new IllegalArgumentException("The number of key positions, key type classes, and key sort directions must be identical.");
//		}
//		for (int i = 0; i < secondarySortKeyPositions.length; i++) {
//			if (secondarySortKeyPositions[i] < 0) {
//				throw new IllegalArgumentException("The key position " + i + " is invalid: " + secondarySortKeyPositions[i]);
//			}
//			if (secondarySortKeyTypes[i] == null || !Key.class.isAssignableFrom(secondarySortKeyTypes[i])) {
//				throw new IllegalArgumentException("The key type " + i + " is null or not implenting the interface " + 
//					Key.class.getName() + ".");
//			}
//		}
//		
//		// write the config
//		config.setInteger(NUM_SS_KEYS, secondarySortKeyPositions.length);
//		for (int i = 0; i < secondarySortKeyPositions.length; i++) {
//			config.setInteger(SS_KEY_POS_PREFIX + i, secondarySortKeyPositions[i]);
//			config.setString(SS_KEY_CLASS_PREFIX + i, secondarySortKeyTypes[i].getName());
//			config.setBoolean(SS_KEY_SORT_DIRECTION_PREFIX + i, secondarySortDirections[i]);
//		}
//	}
}
