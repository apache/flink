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

package org.apache.flink.streaming.python.api.environment;

/**
 * An environment configuration placeholder.
 */
public class PythonEnvironmentConfig {
	public static final String FLINK_PYTHON_DC_ID = "flink";

	public static final String FLINK_PYTHON_PLAN_NAME = "plan.py";

	/**
	 * Holds the path for the local python files cache. Is is set only on the client side by
	 * the python streaming plan binder.
	 */
	public static String pythonTmpCachePath;

	/**
	 * Holds the path in the shared storage at which the python script(s) reside. It is set on the client side
	 * within the execution process.
	 */
	public static String flinkHdfsPath = "hdfs:///tmp/flink"; // "file:/tmp/flink"

}
