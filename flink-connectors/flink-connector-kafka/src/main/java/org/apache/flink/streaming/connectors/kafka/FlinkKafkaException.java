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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.FlinkException;

/**
 * Exception used by {@link FlinkKafkaProducer} and {@link FlinkKafkaConsumer}.
 */
@PublicEvolving
public class FlinkKafkaException extends FlinkException {

	private static final long serialVersionUID = 920269130311214200L;

	private final FlinkKafkaErrorCode errorCode;

	public FlinkKafkaException(FlinkKafkaErrorCode errorCode, String message) {
		super(message);
		this.errorCode = errorCode;
	}

	public FlinkKafkaException(FlinkKafkaErrorCode errorCode, String message, Throwable cause) {
		super(message, cause);
		this.errorCode = errorCode;
	}

	public FlinkKafkaErrorCode getErrorCode() {
		return errorCode;
	}
}
