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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.HadoopUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests that validate the loading of the Hadoop configuration, relative to
 * entries in the Flink configuration and the environment variables.
 */
@SuppressWarnings("deprecation")
public class HadoopConfigLoadingTest {

	private static final String IN_CP_CONFIG_KEY = "cp_conf_key";
	private static final String IN_CP_CONFIG_VALUE = "oompf!";

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void loadFromClasspathByDefault() {
		org.apache.hadoop.conf.Configuration hadoopConf =
				HadoopUtils.getHadoopConfiguration(new Configuration());

		assertEquals(IN_CP_CONFIG_VALUE, hadoopConf.get(IN_CP_CONFIG_KEY, null));
	}

	@Test
	public void loadFromLegacyConfigEntries() throws Exception {
		final String k1 = "shipmate";
		final String v1 = "smooth sailing";

		final String k2 = "pirate";
		final String v2 = "Arrg, yer scurvy dog!";

		final File file1 = tempFolder.newFile("core-site.xml");
		final File file2 = tempFolder.newFile("hdfs-site.xml");

		printConfig(file1, k1, v1);
		printConfig(file2, k2, v2);

		final Configuration cfg = new Configuration();
		cfg.setString(ConfigConstants.HDFS_DEFAULT_CONFIG, file1.getAbsolutePath());
		cfg.setString(ConfigConstants.HDFS_SITE_CONFIG, file2.getAbsolutePath());

		org.apache.hadoop.conf.Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(cfg);

		// contains extra entries
		assertEquals(v1, hadoopConf.get(k1, null));
		assertEquals(v2, hadoopConf.get(k2, null));

		// also contains classpath defaults
		assertEquals(IN_CP_CONFIG_VALUE, hadoopConf.get(IN_CP_CONFIG_KEY, null));
	}

	@Test
	public void loadFromHadoopConfEntry() throws Exception {
		final String k1 = "singing?";
		final String v1 = "rain!";

		final String k2 = "dancing?";
		final String v2 = "shower!";

		final File confDir = tempFolder.newFolder();

		final File file1 = new File(confDir, "core-site.xml");
		final File file2 = new File(confDir, "hdfs-site.xml");

		printConfig(file1, k1, v1);
		printConfig(file2, k2, v2);

		final Configuration cfg = new Configuration();
		cfg.setString(ConfigConstants.PATH_HADOOP_CONFIG, confDir.getAbsolutePath());

		org.apache.hadoop.conf.Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(cfg);

		// contains extra entries
		assertEquals(v1, hadoopConf.get(k1, null));
		assertEquals(v2, hadoopConf.get(k2, null));

		// also contains classpath defaults
		assertEquals(IN_CP_CONFIG_VALUE, hadoopConf.get(IN_CP_CONFIG_KEY, null));
	}

	@Test
	public void loadFromEnvVariables() throws Exception {
		final String k1 = "where?";
		final String v1 = "I'm on a boat";
		final String k2 = "when?";
		final String v2 = "midnight";
		final String k3 = "why?";
		final String v3 = "what do you think?";
		final String k4 = "which way?";
		final String v4 = "south, always south...";
		final String k5 = "how long?";
		final String v5 = "an eternity";
		final String k6 = "for real?";
		final String v6 = "quite so...";

		final File hadoopConfDir = tempFolder.newFolder();

		final File hadoopHome = tempFolder.newFolder();

		final File hadoopHomeConf = new File(hadoopHome, "conf");
		final File hadoopHomeEtc = new File(hadoopHome, "etc/hadoop");

		assertTrue(hadoopHomeConf.mkdirs());
		assertTrue(hadoopHomeEtc.mkdirs());

		final File file1 = new File(hadoopConfDir, "core-site.xml");
		final File file2 = new File(hadoopConfDir, "hdfs-site.xml");
		final File file3 = new File(hadoopHomeConf, "core-site.xml");
		final File file4 = new File(hadoopHomeConf, "hdfs-site.xml");
		final File file5 = new File(hadoopHomeEtc, "core-site.xml");
		final File file6 = new File(hadoopHomeEtc, "hdfs-site.xml");

		printConfig(file1, k1, v1);
		printConfig(file2, k2, v2);
		printConfig(file3, k3, v3);
		printConfig(file4, k4, v4);
		printConfig(file5, k5, v5);
		printConfig(file6, k6, v6);

		final org.apache.hadoop.conf.Configuration hadoopConf;

		final Map<String, String> originalEnv = System.getenv();
		final Map<String, String> newEnv = new HashMap<>(originalEnv);
		newEnv.put("HADOOP_CONF_DIR", hadoopConfDir.getAbsolutePath());
		newEnv.put("HADOOP_HOME", hadoopHome.getAbsolutePath());
		try {
			CommonTestUtils.setEnv(newEnv);
			hadoopConf = HadoopUtils.getHadoopConfiguration(new Configuration());
		}
		finally {
			CommonTestUtils.setEnv(originalEnv);
		}

		// contains extra entries
		assertEquals(v1, hadoopConf.get(k1, null));
		assertEquals(v2, hadoopConf.get(k2, null));
		assertEquals(v3, hadoopConf.get(k3, null));
		assertEquals(v4, hadoopConf.get(k4, null));
		assertEquals(v5, hadoopConf.get(k5, null));
		assertEquals(v6, hadoopConf.get(k6, null));

		// also contains classpath defaults
		assertEquals(IN_CP_CONFIG_VALUE, hadoopConf.get(IN_CP_CONFIG_KEY, null));
	}

	private static void printConfig(File file, String key, String value) throws IOException {
		try (PrintStream out = new PrintStream(new FileOutputStream(file))) {
			out.println("<?xml version=\"1.0\"?>");
			out.println("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>");
			out.println("<configuration>");
			out.println("\t<property>");
			out.println("\t\t<name>" + key + "</name>");
			out.println("\t\t<value>" + value + "</value>");
			out.println("\t</property>");
			out.println("</configuration>");
		}
	}
}
