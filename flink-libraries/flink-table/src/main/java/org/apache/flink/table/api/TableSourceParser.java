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

package org.apache.flink.table.api;

import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser used to fetch a {@link TableFunction}, user can create a parser
 * through a {@link TableFactory}.
 */
public class TableSourceParser {

	private TableFunction<?> parser;

	private List<String> parameters;

	public TableSourceParser(TableFunction<?> parser, List<String> parameters) {
		this.parser = Preconditions.checkNotNull(parser);
		this.parameters = new ArrayList<>(Preconditions.checkNotNull(parameters));
	}

	public TableFunction<?> getParser() {
		return parser;
	}

	public List<String> getParameters() {
		return parameters;
	}
}
