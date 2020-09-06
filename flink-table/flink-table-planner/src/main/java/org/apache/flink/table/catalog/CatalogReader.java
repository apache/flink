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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A thin wrapper around {@link CalciteCatalogReader} that enables providing multiple
 * default paths to look in.
 */
@Internal
public class CatalogReader extends CalciteCatalogReader {
	public CatalogReader(
			CalciteSchema rootSchema,
			List<List<String>> defaultSchema,
			RelDataTypeFactory typeFactory,
			CalciteConnectionConfig config) {
		super(rootSchema,
			SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
			Stream.concat(
				defaultSchema.stream(),
				Stream.of(Collections.<String>emptyList())
			).collect(Collectors.toList()),
			typeFactory,
			config);
	}
}
