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


package org.apache.flink.runtime.testutils.recordutils;

import java.util.Arrays;

import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;

/**
 * A factory for a {@link org.apache.flink.api.common.typeutils.TypeComparator} for {@link Record}. The comparator uses a subset of
 * the fields for the comparison. That subset of fields (positions and types) is read from the
 * supplied configuration.
 */
public class RecordComparatorFactory implements TypeComparatorFactory<Record> {
	
	private static final String NUM_KEYS = "numkeys";
	
	private static final String KEY_POS_PREFIX = "keypos.";
	
	private static final String KEY_CLASS_PREFIX = "keyclass.";
	
	private static final String KEY_SORT_DIRECTION_PREFIX = "key-direction.";
	
	// --------------------------------------------------------------------------------------------
	
	private int[] positions;
	
	private Class<? extends Value>[] types;
	
	private boolean[] sortDirections;
	
	// --------------------------------------------------------------------------------------------
	
	public RecordComparatorFactory() {
		// do nothing, allow to be configured via config
	}
	
	public RecordComparatorFactory(int[] positions, Class<? extends Value>[] types) {
		this(positions, types, null);
	}
	
	public RecordComparatorFactory(int[] positions, Class<? extends Value>[] types, boolean[] sortDirections) {
		if (positions == null || types == null) {
			throw new NullPointerException();
		}
		if (positions.length != types.length) {
			throw new IllegalArgumentException();
		}
		
		this.positions = positions;
		this.types = types;
		
		if (sortDirections == null) {
			this.sortDirections = new boolean[positions.length];
			Arrays.fill(this.sortDirections, true);
		} else if (sortDirections.length != positions.length) {
			throw new IllegalArgumentException();
		} else {
			this.sortDirections = sortDirections;
		}
	}


	@Override
	public void writeParametersToConfig(Configuration config) {
		for (int i = 0; i < this.positions.length; i++) {
			if (this.positions[i] < 0) {
				throw new IllegalArgumentException("The key position " + i + " is invalid: " + this.positions[i]);
			}
			if (this.types[i] == null || !Value.class.isAssignableFrom(this.types[i])) {
				throw new IllegalArgumentException("The key type " + i + " is null or not implenting the interface " + 
					Value.class.getName() + ".");
			}
		}
		
		// write the config
		config.setInteger(NUM_KEYS, this.positions.length);
		for (int i = 0; i < this.positions.length; i++) {
			config.setInteger(KEY_POS_PREFIX + i, this.positions[i]);
			config.setString(KEY_CLASS_PREFIX + i, this.types[i].getName());
			config.setBoolean(KEY_SORT_DIRECTION_PREFIX + i, this.sortDirections[i]);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException {
		// figure out how many key fields there are
		final int numKeyFields = config.getInteger(NUM_KEYS, -1);
		if (numKeyFields < 0) {
			throw new IllegalConfigurationException("The number of keys for the comparator is invalid: " + numKeyFields);
		}
		
		final int[] positions = new int[numKeyFields];
		final Class<? extends Value>[] types = new Class[numKeyFields];
		final boolean[] direction = new boolean[numKeyFields];
		
		// read the individual key positions and types
		for (int i = 0; i < numKeyFields; i++) {
			// next key position
			final int p = config.getInteger(KEY_POS_PREFIX + i, -1);
			if (p >= 0) {
				positions[i] = p;
			} else {
				throw new IllegalConfigurationException("Contained invalid position for key no positions for keys.");
			}
			
			// next key type
			final String name = config.getString(KEY_CLASS_PREFIX + i, null);
			if (name != null) {
				types[i] = (Class<? extends Value>) Class.forName(name, true, cl).asSubclass(Value.class);
			} else {
				throw new IllegalConfigurationException("The key type (" + i +
					") for the comparator is null"); 
			}
			
			// next key sort direction
			direction[i] = config.getBoolean(KEY_SORT_DIRECTION_PREFIX + i, true);
		}
		
		this.positions = positions;
		this.types = types;
		this.sortDirections = direction;
	}
	

	@Override
	public RecordComparator createComparator() {
		return new RecordComparator(this.positions, this.types, this.sortDirections);
	}
}
