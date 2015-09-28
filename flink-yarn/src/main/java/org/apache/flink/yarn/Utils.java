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
package org.apache.flink.yarn;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.mapreduce.security.TokenCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * Utility class that provides helper methods to work with Apache Hadoop YARN.
 */
public final class Utils {
	
	private static final Logger LOG = LoggerFactory.getLogger(Utils.class);


	/**
	 * See documentation
	 */
	public static int calculateHeapSize(int memory, org.apache.flink.configuration.Configuration conf) {
		float memoryCutoffRatio = conf.getFloat(ConfigConstants.YARN_HEAP_CUTOFF_RATIO, ConfigConstants.DEFAULT_YARN_HEAP_CUTOFF_RATIO);
		int minCutoff = conf.getInteger(ConfigConstants.YARN_HEAP_CUTOFF_MIN, ConfigConstants.DEFAULT_YARN_MIN_HEAP_CUTOFF);

		if (memoryCutoffRatio > 1 || memoryCutoffRatio < 0) {
			throw new IllegalArgumentException("The configuration value '" + ConfigConstants.YARN_HEAP_CUTOFF_RATIO + "' must be between 0 and 1. Value given=" + memoryCutoffRatio);
		}
		if (minCutoff > memory) {
			throw new IllegalArgumentException("The configuration value '" + ConfigConstants.YARN_HEAP_CUTOFF_MIN + "' is higher (" + minCutoff + ") than the requested amount of memory " + memory);
		}

		int heapLimit = (int)((float)memory * memoryCutoffRatio);
		if (heapLimit < minCutoff) {
			heapLimit = minCutoff;
		}
		return memory - heapLimit;
	}

	
	public static void setupEnv(Configuration conf, Map<String, String> appMasterEnv) {
		addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), Environment.PWD.$() + File.separator + "*");
		for (String c: conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
		}
	}
	
	
	/**
	 * 
	 * @return Path to remote file (usually hdfs)
	 * @throws IOException
	 */
	public static Path setupLocalResource(Configuration conf, FileSystem fs, String appId, Path localRsrcPath, LocalResource appMasterJar, Path homedir)
			throws IOException {
		// copy to HDFS
		String suffix = ".flink/" + appId + "/" + localRsrcPath.getName();
		
		Path dst = new Path(homedir, suffix);
		
		LOG.info("Copying from " + localRsrcPath + " to " + dst );
		fs.copyFromLocalFile(localRsrcPath, dst);
		registerLocalResource(fs, dst, appMasterJar);
		return dst;
	}
	
	public static void registerLocalResource(FileSystem fs, Path remoteRsrcPath, LocalResource localResource) throws IOException {
		FileStatus jarStat = fs.getFileStatus(remoteRsrcPath);
		localResource.setResource(ConverterUtils.getYarnUrlFromURI(remoteRsrcPath.toUri()));
		localResource.setSize(jarStat.getLen());
		localResource.setTimestamp(jarStat.getModificationTime());
		localResource.setType(LocalResourceType.FILE);
		localResource.setVisibility(LocalResourceVisibility.APPLICATION);
	}

	public static void setTokensFor(ContainerLaunchContext amContainer, Path[] paths, Configuration conf) throws IOException {
		Credentials credentials = new Credentials();
		// for HDFS
		TokenCache.obtainTokensForNamenodes(credentials, paths, conf);
		// for user
		UserGroupInformation currUsr = UserGroupInformation.getCurrentUser();
		
		Collection<Token<? extends TokenIdentifier>> usrTok = currUsr.getTokens();
		for(Token<? extends TokenIdentifier> token : usrTok) {
			final Text id = new Text(token.getIdentifier());
			LOG.info("Adding user token " + id + " with " + token);
			credentials.addToken(id, token);
		}
		DataOutputBuffer dob = new DataOutputBuffer();
		credentials.writeTokenStorageToStream(dob);

		if(LOG.isDebugEnabled()) {
			LOG.debug("Wrote tokens. Credentials buffer length: " + dob.getLength());
		}

		ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
		amContainer.setTokens(securityTokens);
	}
	
	public static void logFilesInCurrentDirectory(final Logger logger) {
		new File(".").list(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				logger.info(dir.getAbsolutePath() + "/" + name);
				return true;
			}
		});
	}
	
	/**
	 * Copied method from org.apache.hadoop.yarn.util.Apps
	 * It was broken by YARN-1824 (2.4.0) and fixed for 2.4.1
	 * by https://issues.apache.org/jira/browse/YARN-1931
	 */
	public static void addToEnvironment(Map<String, String> environment,
			String variable, String value) {
		String val = environment.get(variable);
		if (val == null) {
			val = value;
		} else {
			val = val + File.pathSeparator + value;
		}
		environment.put(StringInterner.weakIntern(variable),
				StringInterner.weakIntern(val));
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private Utils() {
		throw new RuntimeException();
	}
}
