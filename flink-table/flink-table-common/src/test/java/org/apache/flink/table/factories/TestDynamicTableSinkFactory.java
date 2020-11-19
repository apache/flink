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

package org.apache.flink.table.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import java.util.Set;

/**
 * Test implementations for {@link DynamicTableSinkFactory}. The source and sink "test" connector
 * is separated into two factory implementation.
 *
 * @see TestDynamicTableSourceFactory
 */
public final class TestDynamicTableSinkFactory implements DynamicTableSinkFactory {

	public static final String IDENTIFIER = "test";

	private static final TestDynamicTableFactory delegate = new TestDynamicTableFactory();

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		return delegate.createDynamicTableSink(context);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return delegate.requiredOptions();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return delegate.optionalOptions();
	}

}
