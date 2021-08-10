/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Flink sql parser.
 *
 * <p>This module contains the DDLs and some custom DMLs for Apache Flink.
 *
 * <p>Most of the sql grammars belong to sql standard or Flink's dialect. To support a new sql
 * dialect, add a new sql conformance to {@link
 * org.apache.flink.sql.parser.validate.FlinkSqlConformance}, then use this sql conformance to make
 * context aware decisions in parse block. See the usage of {@link
 * org.apache.flink.sql.parser.validate.FlinkSqlConformance#HIVE} in {@code parserImpls.ftl}.
 *
 * <p>To use a specific sql dialect for the parser, config the parser to the specific sql
 * conformance with a code snippet like below:
 *
 * <blockquote>
 *
 * <pre>
 *   SqlParser.create(source,
 *   		SqlParser.configBuilder()
 *   			.setParserFactory(parserImplFactory())
 * 				.setQuoting(Quoting.DOUBLE_QUOTE)
 * 				.setUnquotedCasing(Casing.TO_UPPER)
 * 				.setQuotedCasing(Casing.UNCHANGED)
 * 				.setConformance(conformance0) // the sql conformance you want use.
 * 				.build());
 * </pre>
 *
 * </blockquote>
 */
@PackageMarker
package org.apache.flink.sql.parser;

import org.apache.calcite.avatica.util.PackageMarker;
