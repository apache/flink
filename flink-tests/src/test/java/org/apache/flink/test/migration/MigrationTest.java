/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.migration;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;

import java.io.File;

public class MigrationTest {

	public static void main(String[] args) throws Exception {

		int numTms = 1;
		int numSlots = 8;

		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlots);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTms);

		boolean localActorSystem = false;

		LocalFlinkMiniCluster cluster = new LocalFlinkMiniCluster(config, localActorSystem);

		boolean waitForTms = true;
		cluster.start(waitForTms);


//		File jarFile = new File("/Users/stefan/Downloads/ssm-1.2-SNAPSHOT-fs.jar");
		File jarFile = new File("/Users/stefan/Downloads/streaming-state-machine-0.1-SNAPSHOT (1).jar");
//		File jarFile = new File("/Users/stefan/Downloads/streaming-state-machine-0.1-SNAPSHOT (2).jar");
		PackagedProgram program = new PackagedProgram(jarFile);

//		String savepointFilename = "file:///Users/stefan/Downloads/ssm-1.1-savepoint/savepoint-7c7d2a2d1c42";
		String savepointFilename = "file:///Users/stefan/Downloads/savepoint-8cb7ea19a289";
//		String savepointFilename = "file:///Users/stefan/Downloads/rocksdb-savepoint-fully-async/savepoint-86bda15a2073";
		program.setSavepointPath(savepointFilename);

		new StandaloneClusterClient(cluster.configuration()).run(program, 1);

		cluster.awaitTermination();
	}
}
