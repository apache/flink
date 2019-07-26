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

import org.apache.flink.table.catalog.ExternalCatalog;

import java.util.Map;

/**
 * A factory to create configured external catalog instances based on string-based properties. See
 * also {@link TableFactory} for more information.
 *
 * @deprecated use {@link CatalogFactory} instead.
 */
@Deprecated
public interface ExternalCatalogFactory extends TableFactory {

	/**
	 * Creates and configures an {@link ExternalCatalog} using the given properties.
	 *
	 * @param properties normalized properties describing an external catalog.
	 * @return the configured external catalog.
	 */
	ExternalCatalog createExternalCatalog(Map<String, String> properties);
}
