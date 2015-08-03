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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unmodifiable version of the Configuration class
 */
public class UnmodifiableConfiguration extends Configuration {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(UnmodifiableConfiguration.class);

	public UnmodifiableConfiguration(Configuration config) {
		super();
		super.addAll(config);
	}

	// --------------------------------------------------------------------------------------------
	//  All setter methods must fail.
	// --------------------------------------------------------------------------------------------

	@Override
	public final void setClass(String key, Class<?> klazz) {
		error();
	}

	@Override
	public final void setString(String key, String value) {
		error();
	}

	@Override
	public final void setInteger(String key, int value) {
		error();
	}

	@Override
	public final void setLong(String key, long value) {
		error();
	}

	@Override
	public final void setBoolean(String key, boolean value) {
		error();
	}

	@Override
	public final void setFloat(String key, float value) {
		error();
	}

	@Override
	public final void setDouble(String key, double value) {
		error();
	}

	@Override
	public final void setBytes(String key, byte[] bytes) {
		error();
	}

	@Override
	public final void addAll(Configuration other) {
		error();
	}

	@Override
	public final void addAll(Configuration other, String prefix) {
		error();
	}

	@Override
	<T> void setValueInternal(String key, T value){
		error();
	}

	private final void error(){
		throw new UnsupportedOperationException("The unmodifiable configuration object doesn't allow set methods.");
	}
	
}
