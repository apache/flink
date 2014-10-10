/**
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class YarnTestBase {
	private static final Logger LOG = LoggerFactory.getLogger(YarnClientIT.class);
	protected static MiniYARNCluster yarnCluster = null;
	protected static File flinkConfFile;
	protected static File yarnSiteXML;
	protected static String uberJarLocation;
	protected static YarnConfManager yarnConfManager = new YarnConfManager();

	protected static void setEnv(Map<String, String> newenv) {
		try {
			Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
			Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
			theEnvironmentField.setAccessible(true);
			Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
			env.putAll(newenv);
			Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
			theCaseInsensitiveEnvironmentField.setAccessible(true);
			Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
			cienv.putAll(newenv);
		} catch (NoSuchFieldException e) {
			try {
				Class[] classes = Collections.class.getDeclaredClasses();
				Map<String, String> env = System.getenv();
				for (Class cl : classes) {
					if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
						Field field = cl.getDeclaredField("m");
						field.setAccessible(true);
						Object obj = field.get(env);
						Map<String, String> map = (Map<String, String>) obj;
						map.clear();
						map.putAll(newenv);
					}
				}
			} catch (Exception e2) {
				throw new RuntimeException(e2);
			}
		} catch (Exception e1) {
			throw new RuntimeException(e1);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@BeforeClass
	public static void setup() {
		File uberjar = new File("../flink-dist/target/flink-dist-0.7-incubating-SNAPSHOT-yarn-uberjar.jar");
		if (!uberjar.exists()) {
			uberjar = new File("./flink-dist/target/flink-dist-0.7-incubating-SNAPSHOT-yarn-uberjar.jar");
		}

		uberJarLocation = uberjar.getAbsolutePath();

		try {
			LOG.info("Starting up MiniYARN cluster");
			if (yarnCluster == null) {
				yarnCluster = new MiniYARNCluster(YarnClientIT.class.getName(), 1, 1, 1);
				Configuration conf = yarnConfManager.getMiniClusterConf();
				yarnCluster.init(conf);
				yarnCluster.start();
			}

			Thread.sleep(5000);

			Configuration miniyarn_conf = yarnCluster.getConfig();
			Map flink_conf = yarnConfManager.getDefaultFlinkConfig();

			yarnSiteXML = yarnConfManager.createYarnSiteConfig(miniyarn_conf);
			flinkConfFile = yarnConfManager.createConfigFile(flink_conf);

			Map<String, String> map = new HashMap<String, String>(System.getenv());
			map.put("FLINK_CONF_DIR", flinkConfFile.getParentFile().getAbsolutePath());
			map.put("YARN_CONF_DIR", yarnSiteXML.getParentFile().getAbsolutePath());
			setEnv(map);

			Assert.assertTrue(yarnCluster.getServiceState() == Service.STATE.STARTED);
		} catch (InterruptedException e){
			LOG.error("Thread.sleep was interrupted during test setup");
		} catch (Exception ex) {
			LOG.error("setup failure", ex);
			Assert.assertEquals(null, ex);
		}
	}

	@AfterClass
	public static void tearDown() {
		//shutdown YARN cluster
		if (yarnCluster != null) {
			LOG.info("shutdown MiniYarn cluster");
			yarnCluster.stop();
			yarnCluster = null;
		}

		if (flinkConfFile != null && flinkConfFile.exists()) {
			flinkConfFile.delete();
		}

		if (yarnSiteXML != null && yarnSiteXML.exists()) {
			yarnSiteXML.delete();
		}
	}

	protected Exception executeExClient(ClientCallableExTesting client, int timeout) {
		ExecutorService serv = Executors.newFixedThreadPool(1);
		Future<Exception> future = serv.submit(client);

		Exception ex = null;
		try {
			ex = future.get(timeout, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		serv.shutdown();
		return ex;
	}
}
