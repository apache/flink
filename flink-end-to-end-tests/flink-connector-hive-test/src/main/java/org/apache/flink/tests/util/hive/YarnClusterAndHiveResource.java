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

package org.apache.flink.tests.util.hive;

import org.apache.flink.util.ExternalResource;

import java.io.IOException;

/**
 * Generic interface for interacting with YarnCluster and hive service.
 */
public interface YarnClusterAndHiveResource extends ExternalResource {

	void startYarnClusterAndHiveServer() throws IOException;

	void stopYarnClusterAndHiveServer() throws IOException;

	String execHiveSql(String sql) throws IOException;

	void copyLocalFileToHiveGateWay(String localPath, String remotePath) throws IOException;

	void copyLocalFileToYarnMaster(String localPath, String remotePath) throws IOException;

}
