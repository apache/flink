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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.throwable.ThrowableAnnotation;
import org.apache.flink.runtime.throwable.ThrowableClassifier;
import org.apache.flink.runtime.throwable.ThrowableType;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test throwable classifier
 * */
public class ThrowableClassifierTest extends TestLogger {

	@Test
	public void testThrowableType_NonRecoverable() {
		assertEquals(ThrowableType.NonRecoverableError,
			ThrowableClassifier.getThrowableType(new SuppressRestartsException(new Exception(""))));

		assertEquals(ThrowableType.NonRecoverableError,
			ThrowableClassifier.getThrowableType(new NoResourceAvailableException()));
	}

	@Test
	public void testThrowableType_Recoverable() {
		assertEquals(ThrowableType.RecoverableError,
			ThrowableClassifier.getThrowableType(new Exception("")));
		assertEquals(ThrowableType.RecoverableError,
			ThrowableClassifier.getThrowableType(new ThrowableType_RecoverableFailure_Exception()));
	}

	@Test
	public void testThrowableType_EnvironmentError() {
		assertEquals(ThrowableType.EnvironmentError,
			ThrowableClassifier.getThrowableType(new ThrowableType_EnvironmentError_Exception()));
	}

	@Test
	public void testThrowableType_PartitionDataMissingError() {
		assertEquals(ThrowableType.PartitionDataMissingError,
			ThrowableClassifier.getThrowableType(new ThrowableType_PartitionDataMissingError_Exception()));
	}

	@Test
	public void testThrowableType_InheritError() {
		assertEquals(ThrowableType.PartitionDataMissingError,
			ThrowableClassifier.getThrowableType(new Sub_ThrowableType_PartitionDataMissingError_Exception()));
	}

	@ThrowableAnnotation(ThrowableType.PartitionDataMissingError)
	private class ThrowableType_PartitionDataMissingError_Exception extends Exception {
	}

	@ThrowableAnnotation(ThrowableType.EnvironmentError)
	private class ThrowableType_EnvironmentError_Exception extends Exception {
	}

	@ThrowableAnnotation(ThrowableType.RecoverableError)
	private class ThrowableType_RecoverableFailure_Exception extends Exception {
	}

	private class Sub_ThrowableType_PartitionDataMissingError_Exception extends ThrowableType_PartitionDataMissingError_Exception {
	}
}
