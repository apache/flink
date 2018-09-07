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

package org.apache.flink.runtime.throwable;

import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * given a exception do the classification.
 */
public class ThrowableClassifier {

	//TODO: add more NON_RECOVERABLE exception here
	private static final Set<Class> NON_RECOVERABLE = new HashSet<>(Arrays.asList(
		SuppressRestartsException.class,
		NoResourceAvailableException.class
	));

	//TODO: add more PARTITION_MISSING_ERROR exception here
	private static final Set<Class> PARTITION_MISSING_ERROR = new HashSet<>(Arrays.asList(
	));

	//TODO: add more ENVIRONMENT_ERROR exception here
	private static final Set<Class> ENVIRONMENT_ERROR = new HashSet<>(Arrays.asList(
	));

	private static final boolean isInstanceOf(Set<Class> category, Throwable cause){
		return category.contains(cause)
			|| category.stream().anyMatch(c -> c.isAssignableFrom(cause.getClass()));
	}

	/**
	 * classify the exceptions, that will be handled different failover logic.
	 * @param cause
	 * @return
	 */
	public static ThrowableType getThrowableType(Throwable cause) {

		if (isInstanceOf(NON_RECOVERABLE, cause)) {
			return ThrowableType.NonRecoverable;
		} else if (isInstanceOf(PARTITION_MISSING_ERROR, cause)) {
			return ThrowableType.PartitionDataMissingError;
		} else if (isInstanceOf(ENVIRONMENT_ERROR, cause)) {
			return ThrowableType.EnvironmentError;
		}
		return ThrowableType.Other;
	}
}
