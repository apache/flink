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

import org.apache.flink.annotation.PublicEvolving;

import java.util.List;
import java.util.Map;

/**
 * A factory interface for components that enables further disambiguating in case
 * there are multiple matching implementations present.
 */
@PublicEvolving
public interface ComponentFactory extends TableFactory {
	/**
	 * Specifies a context of optional parameters that if exist should have the
	 * given values. This enables further disambiguating if there are multiple
	 * factories that meet the {@link #requiredContext()} and {@link #supportedProperties()}.
	 *
	 * <p><b>NOTE:</b> All the property keys should be included in {@link #supportedProperties()}.
 	 *
	 * @return optional properties to disambiguate factories
	 */
	Map<String, String> optionalContext();

	@Override
	Map<String, String> requiredContext();

	/**
	 * {@inheritDoc}
	 *
	 * <p><b>NOTE:</b> All the property keys from {@link #optionalContext()} should also be included.
	 */
	@Override
	List<String> supportedProperties();
}
