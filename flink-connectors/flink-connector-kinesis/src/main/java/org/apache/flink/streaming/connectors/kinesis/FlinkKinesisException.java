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

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.annotation.Internal;

/**
 * A {@link RuntimeException} wrapper indicating the exception was thrown from this connector.
 * This class is abstract, semantic subclasses should be created to indicate the type of exception.
 */
@Internal
public abstract class FlinkKinesisException extends RuntimeException {

	public FlinkKinesisException(final String message) {
		super(message);
	}

	public FlinkKinesisException(final String message, final Throwable cause) {
		super(message, cause);
	}

	/**
	 * A semantic {@link RuntimeException} thrown to indicate timeout errors in the Kinesis connector.
	 */
	@Internal
	public static class FlinkKinesisTimeoutException extends FlinkKinesisException {

		public FlinkKinesisTimeoutException(String message) {
			super(message);
		}
	}

}
