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

package org.apache.flink.table.catalog.hive.factories;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogDescriptor;
import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.hive.conf.HiveConf;
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

/**
 * Test for {@link HiveCatalog} created by {@link HiveCatalogFactory}.
 */
public class HiveCatalogFactoryTest extends TestLogger {
	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void test() {
		final String catalogName = "mycatalog";

		final HiveCatalog expectedCatalog = HiveTestUtils.createHiveCatalog(catalogName, null);

		final CatalogDescriptor catalogDescriptor = new HiveCatalogDescriptor();

		final Map<String, String> properties = catalogDescriptor.toProperties();

		final Catalog actualCatalog = TableFactoryService.find(CatalogFactory.class, properties)
			.createCatalog(catalogName, properties);

		checkEquals(expectedCatalog, (HiveCatalog) actualCatalog);
	}

	@Test
	public void testLoadHDFSConfigFromEnv() throws IOException {
		final String k1 = "what is connector?";
		final String v1 = "Hive";
		final String catalogName = "HiveCatalog";

		// set HADOOP_CONF_DIR env
		final File hadoopConfDir = tempFolder.newFolder();
		final File hdfsSiteFile = new File(hadoopConfDir, "hdfs-site.xml");
		writeProperty(hdfsSiteFile, k1, v1);
		final Map<String, String> originalEnv = System.getenv();
		final Map<String, String> newEnv = new HashMap<>(originalEnv);
		newEnv.put("HADOOP_CONF_DIR", hadoopConfDir.getAbsolutePath());
		CommonTestUtils.setEnv(newEnv);

		// create HiveCatalog use the Hadoop Configuration
		final CatalogDescriptor catalogDescriptor = new HiveCatalogDescriptor();
		final Map<String, String> properties = catalogDescriptor.toProperties();
		final HiveConf hiveConf;
		try {
			final HiveCatalog hiveCatalog = (HiveCatalog) TableFactoryService.find(CatalogFactory.class, properties)
				.createCatalog(catalogName, properties);
			hiveConf = hiveCatalog.getHiveConf();
		} finally {
			// set the Env back
			CommonTestUtils.setEnv(originalEnv);
		}
		//validate the result
		assertEquals(v1, hiveConf.get(k1, null));
	}

	private static void checkEquals(HiveCatalog c1, HiveCatalog c2) {
		// Only assert a few selected properties for now
		assertEquals(c1.getName(), c2.getName());
		assertEquals(c1.getDefaultDatabase(), c2.getDefaultDatabase());
	}

	private static void writeProperty(File file, String key, String value) throws IOException {
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
