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

package org.apache.flink.table.catalog.exceptions;

import org.apache.flink.table.catalog.ObjectPath;

/**
 * Exception for trying to operate on a function that doesn't exist.
 */
public class FunctionNotExistException extends Exception {

	private static final String MSG = "Function %s does not exist in Catalog %s.";

	public FunctionNotExistException(String catalogName, ObjectPath functionPath) {
		this(catalogName, functionPath, null);
	}

	public FunctionNotExistException(String catalogName, ObjectPath functionPath, Throwable cause) {
		super(String.format(MSG, functionPath.getFullName(), catalogName), cause);
	}

}
