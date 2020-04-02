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

package org.apache.flink.tests.util.flink;

/**
 * Represents a move/copy operation for a jar.
 */
class JarOperation {

	private final String jarNamePrefix;
	private final JarLocation source;
	private final JarLocation target;
	private final OperationType operationType;

	JarOperation(String jarNamePrefix, JarLocation source, JarLocation target, OperationType operationType) {
		this.jarNamePrefix = jarNamePrefix;
		this.source = source;
		this.target = target;
		this.operationType = operationType;
	}

	public String getJarNamePrefix() {
		return jarNamePrefix;
	}

	public JarLocation getSource() {
		return source;
	}

	public JarLocation getTarget() {
		return target;
	}

	public OperationType getOperationType() {
		return operationType;
	}

	public enum OperationType {
		COPY,
		MOVE;
	}
}
