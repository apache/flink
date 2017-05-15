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

package org.apache.flink.yarn.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.FsNegativeRunningJobsRegistry;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;

import java.io.IOException;

/**
 * Abstract base class for the high availability services for Flink YARN applications that support
 * no master fail over.
 *
 * <p>Internally, these services put their recovery data into YARN's working directory,
 * except for checkpoints, which are in the configured checkpoint directory. That way,
 * checkpoints can be resumed with a new job/application, even if the complete YARN application
 * is killed and cleaned up.
 */
public abstract class AbstractYarnNonHaServices extends YarnHighAvailabilityServices {

	// ------------------------------------------------------------------------

	/**
	 * Creates new YARN high-availability services, configuring the file system and recovery
	 * data directory based on the working directory in the given Hadoop configuration.
	 *
	 * <p>This class requires that the default Hadoop file system configured in the given
	 * Hadoop configuration is an HDFS.
	 *
	 * @param config     The Flink configuration of this component / process.
	 * @param hadoopConf The Hadoop configuration for the YARN cluster.
	 *
	 * @throws IOException Thrown, if the initialization of the Hadoop file system used by YARN fails.
	 */
	protected AbstractYarnNonHaServices(
			Configuration config,
			org.apache.hadoop.conf.Configuration hadoopConf) throws IOException {
		super(config, hadoopConf);
	}

	// ------------------------------------------------------------------------
	//  Services
	// ------------------------------------------------------------------------

	@Override
	public RunningJobsRegistry getRunningJobsRegistry() throws IOException {
		enter();
		try {
			// IMPORTANT: The registry must NOT place its data in a directory that is
			// cleaned up by these services.
			return new FsNegativeRunningJobsRegistry(flinkFileSystem, workingDirectory);
		}
		finally {
			exit();
		}
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		enter();
		try {
			return new StandaloneCheckpointRecoveryFactory();
		}
		finally {
			exit();
		}
	}

	@Override
	public SubmittedJobGraphStore getSubmittedJobGraphStore() throws Exception {
		throw new UnsupportedOperationException("These High-Availability Services do not support storing job graphs");
	}
}
