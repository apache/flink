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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.FlinkRuntimeException;

/**
 * An exception that indicates that a time characteristic was used that is not supported in the
 * current operation.
 */
@PublicEvolving
public class UnsupportedTimeCharacteristicException extends FlinkRuntimeException {

	private static final long serialVersionUID = -8109094930338075819L;

	public UnsupportedTimeCharacteristicException(String message) {
		super(message);
	}
}
