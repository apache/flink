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
package org.apache.flink.test.state.operator.restore;

/**
 * Enum to control function behavior for the different test stages.
 * 
 * {@link ExecutionMode#GENERATE} should be used when creating the 1.2 savepoint.
 * {@link ExecutionMode#MIGRATE} should be used when migrating the 1.2 savepoint to 1.3.
 * {@link ExecutionMode#RESTORE} should be used when restoring from the migrated 1.3 savepoint.
 */
public enum ExecutionMode {
	GENERATE,
	MIGRATE,
	RESTORE
}
