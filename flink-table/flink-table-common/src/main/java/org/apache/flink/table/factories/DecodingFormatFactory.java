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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;

/**
 * Base interface for configuring a {@link DecodingFormat} for {@link ScanTableSource} and {@link LookupTableSource}.
 *
 * <p>Depending on the kind of external system, a connector might support different encodings for
 * reading and writing rows. This interface helps in making such formats pluggable.
 *
 * <p>The created {@link Format} instance is an intermediate representation that can be used to construct
 * runtime implementation in a later step.
 *
 * @see FactoryUtil#createTableFactoryHelper(DynamicTableFactory, DynamicTableFactory.Context)
 *
 * @param <I> runtime interface needed by the table source
 */
@PublicEvolving
public interface DecodingFormatFactory<I> extends Factory {

	/**
	 * Creates a format from the given context and format options.
	 *
	 * <p>The format options have been projected to top-level options (e.g. from {@code format.ignore-errors}
	 * to {@code ignore-errors}).
	 */
	DecodingFormat<I> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions);
}
