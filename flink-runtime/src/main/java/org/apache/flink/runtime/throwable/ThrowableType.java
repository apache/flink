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

/**
 * */
public enum ThrowableType {

	/**
	 * this indicates error that would not succeed even with retry, such as DivideZeroExeception.
	 * No failover should be attempted with such error. Instead, the job should fail immediately.
	 */
	NonRecoverableError,

	/**
	 * data consumption error, which indicates that we should revoke the producer.
	 * */
	PartitionDataMissingError,

	/**
	 * this indicates error related to running environment, such as hardware error, service issue, in which case we should consider blacklist the machine.
	 * */
	EnvironmentError,

	/**
	 * this indicates other errors that is recoverable.
	 * */
	Other
}
