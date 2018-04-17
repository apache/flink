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

package org.apache.flink.types.parser;

/**
 * Supertype for factories that supply instances of {@link FieldParser} class.
 * @param <T> target type meant to be produced by the {@link FieldParser} instance.
 */
public interface ParserFactory<T> {

	/**
	 * Gives an insight of a {@link FieldParser} instance's type to be created by this factory.
	 * @return a class of {@link FieldParser} instances provided by this factory.
	 */
	Class<? extends FieldParser<T>> getParserType();

	/**
	 * Creates a dedicated instance of {@link FieldParser}.
	 * @return an instance of {@link FieldParser} covered by this factory.
	 */
	FieldParser<T> create();

}
