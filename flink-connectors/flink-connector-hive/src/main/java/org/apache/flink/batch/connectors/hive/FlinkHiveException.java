/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.batch.connectors.hive;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Exception used by {@link HiveTableInputFormat} and {@link HiveTableOutputFormat}.
 */
@PublicEvolving
public class FlinkHiveException extends RuntimeException {

	private static final long serialVersionUID = 920269130311214200L;

	public FlinkHiveException(Throwable cause) {
		super(cause);
	}

	public FlinkHiveException(String message, Throwable cause) {
		super(message, cause);
	}
}
