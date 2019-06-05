/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.error.SqlParseException;

/** An remark interface which should be inherited by extended sql nodes which are not supported
 * by Calcite core parser.
 *
 * <p>We need this to customize our validation rules combined with the rules defined in
 * {@link org.apache.calcite.sql.validate.SqlValidatorImpl}.
 */
public interface ExtendedSqlNode {
	void validate() throws SqlParseException;
}
