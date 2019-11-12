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

package org.apache.flink.runtime.clusterframework.overlays;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

/**
 * Overlays a Hadoop configuration into a container, based on a supplied Hadoop
 * configuration directory.
 *
 * The following files are copied to the container:
 *  - hadoop/conf/core-site.xml
 *  - hadoop/conf/hdfs-site.xml
 *
 * The following environment variables are set in the container:
 *  - HADOOP_CONF_DIR
 *
 * The following Flink configuration entries are updated:
 *  - fs.hdfs.hadoopconf
 */
public class HadoopConfOverlay implements ContainerOverlay {

	private static final Logger LOG = LoggerFactory.getLogger(HadoopConfOverlay.class);

	/**
	 * The (relative) directory into which the Hadoop conf is copied.
	 */
	static final Path TARGET_CONF_DIR = new Path("hadoop/conf");

	final File hadoopConfDir;

	public HadoopConfOverlay(@Nullable File hadoopConfDir) {
		this.hadoopConfDir = hadoopConfDir;
	}

	@Override
	public void configure(ContainerSpecification container) throws IOException {

		if(hadoopConfDir == null) {
			return;
		}

		File coreSitePath = new File(hadoopConfDir, "core-site.xml");
		File hdfsSitePath = new File(hadoopConfDir, "hdfs-site.xml");

		container.getEnvironmentVariables().put("HADOOP_CONF_DIR", TARGET_CONF_DIR.toString());
		container.getFlinkConfiguration().setString(ConfigConstants.PATH_HADOOP_CONFIG, TARGET_CONF_DIR.toString());

		container.getArtifacts().add(ContainerSpecification.Artifact
			.newBuilder()
			.setSource(new Path(coreSitePath.toURI()))
			.setDest(new Path(TARGET_CONF_DIR, coreSitePath.getName()))
			.setCachable(true)
			.build());

		container.getArtifacts().add(ContainerSpecification.Artifact
			.newBuilder()
			.setSource(new Path(hdfsSitePath.toURI()))
			.setDest(new Path(TARGET_CONF_DIR, hdfsSitePath.getName()))
			.setCachable(true)
			.build());
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * A builder for the {@link HadoopConfOverlay}.
	 */
	public static class Builder {

		File hadoopConfDir;

		/**
		 * Configures the overlay using the current environment's Hadoop configuration.
		 *
		 * The following locations are checked for a Hadoop configuration:
		 *  - (conf) fs.hdfs.hadoopconf
		 *  - (env)  HADOOP_CONF_DIR
		 *  - (env)  HADOOP_HOME/conf
		 *  - (env)  HADOOP_HOME/etc/hadoop
		 *
		 */
		public Builder fromEnvironment(Configuration globalConfiguration) {

			String[] possibleHadoopConfPaths = new String[4];
			possibleHadoopConfPaths[0] = globalConfiguration.getString(ConfigConstants.PATH_HADOOP_CONFIG, null);
			possibleHadoopConfPaths[1] = System.getenv("HADOOP_CONF_DIR");

			if (System.getenv("HADOOP_HOME") != null) {
				possibleHadoopConfPaths[2] = System.getenv("HADOOP_HOME")+"/conf";
				possibleHadoopConfPaths[3] = System.getenv("HADOOP_HOME")+"/etc/hadoop"; // hadoop 2.2
			}

			for (String possibleHadoopConfPath : possibleHadoopConfPaths) {
				if (possibleHadoopConfPath != null) {
					File confPath = new File(possibleHadoopConfPath);

					File coreSitePath = new File(confPath, "core-site.xml");
					File hdfsSitePath = new File(confPath, "hdfs-site.xml");

					if (coreSitePath.exists() && hdfsSitePath.exists()) {
						this.hadoopConfDir = confPath;
						break;
					}
				}
			}

			if(hadoopConfDir == null) {
				LOG.warn("Unable to locate a Hadoop configuration; HDFS will use defaults.");
			}

			return this;
		}

		public HadoopConfOverlay build() {
			return new HadoopConfOverlay(hadoopConfDir);
		}
	}
}

