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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for {@link JDBCLookupFunction} and {@link JDBCAsyncLookupFunction}.
 */
public class JDBCLookupBuilder {

	private JDBCOptions options;
	private JDBCLookupOptions lookupOptions;
	private String[] fieldNames;
	private TypeInformation[] fieldTypes;
	private String[] keyNames;

	/**
	 * required, jdbc options.
	 */
	public JDBCLookupBuilder setOptions(JDBCOptions options) {
		this.options = options;
		return this;
	}

	/**
	 * optional, lookup related options.
	 */
	public JDBCLookupBuilder setLookupOptions(JDBCLookupOptions lookupOptions) {
		this.lookupOptions = lookupOptions;
		return this;
	}

	/**
	 * required, field names of this jdbc table.
	 */
	public JDBCLookupBuilder setFieldNames(String[] fieldNames) {
		this.fieldNames = fieldNames;
		return this;
	}

	/**
	 * required, field types of this jdbc table.
	 */
	public JDBCLookupBuilder setFieldTypes(TypeInformation[] fieldTypes) {
		this.fieldTypes = fieldTypes;
		return this;
	}

	/**
	 * required, key names to query this jdbc table.
	 */
	public JDBCLookupBuilder setKeyNames(String[] keyNames) {
		this.keyNames = keyNames;
		return this;
	}

	public JDBCLookupFunction buildLookup() {
		checkNotNull(options, "No JDBCOptions supplied.");
		if (lookupOptions == null) {
			lookupOptions = JDBCLookupOptions.builder().build();
		}
		checkNotNull(fieldNames, "No fieldNames supplied.");
		checkNotNull(fieldTypes, "No fieldTypes supplied.");
		checkNotNull(keyNames, "No keyNames supplied.");

		return new JDBCLookupFunction(options, lookupOptions, fieldNames, fieldTypes, keyNames);
	}

	public JDBCAsyncLookupFunction buildAsyncLookup() {
		checkNotNull(options, "No JDBCOptions supplied.");
		if (lookupOptions == null) {
			lookupOptions = JDBCLookupOptions.builder().build();
		}
		checkNotNull(fieldNames, "No fieldNames supplied.");
		checkNotNull(fieldTypes, "No fieldTypes supplied.");
		checkNotNull(keyNames, "No keyNames supplied.");

		return new JDBCAsyncLookupFunction(options, lookupOptions, fieldNames, fieldTypes, keyNames);
	}

}
