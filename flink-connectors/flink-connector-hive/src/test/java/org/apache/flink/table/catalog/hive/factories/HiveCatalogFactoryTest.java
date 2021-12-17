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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/** Test for {@link HiveCatalog} created by {@link HiveCatalogFactory}. */
public class HiveCatalogFactoryTest extends TestLogger {

    private static final URL CONF_DIR =
            Thread.currentThread().getContextClassLoader().getResource("test-catalog-factory-conf");

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCreateHiveCatalog() {
        final String catalogName = "mycatalog";

        final HiveCatalog expectedCatalog = HiveTestUtils.createHiveCatalog(catalogName, null);

        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), HiveCatalogFactoryOptions.IDENTIFIER);
        options.put(HiveCatalogFactoryOptions.HIVE_CONF_DIR.key(), CONF_DIR.getPath());

        final Catalog actualCatalog =
                FactoryUtil.createCatalog(
                        catalogName, options, null, Thread.currentThread().getContextClassLoader());

        assertEquals(
                "dummy-hms",
                ((HiveCatalog) actualCatalog)
                        .getHiveConf()
                        .getVar(HiveConf.ConfVars.METASTOREURIS));
        checkEquals(expectedCatalog, (HiveCatalog) actualCatalog);
    }

    @Test
    public void testCreateHiveCatalogWithHadoopConfDir() throws IOException {
        final String catalogName = "mycatalog";

        final String hadoopConfDir = tempFolder.newFolder().getAbsolutePath();
        final File mapredSiteFile = new File(hadoopConfDir, "mapred-site.xml");
        final String mapredKey = "mapred.site.config.key";
        final String mapredVal = "mapred.site.config.val";
        writeProperty(mapredSiteFile, mapredKey, mapredVal);

        final HiveCatalog expectedCatalog =
                HiveTestUtils.createHiveCatalog(
                        catalogName, CONF_DIR.getPath(), hadoopConfDir, null);

        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), HiveCatalogFactoryOptions.IDENTIFIER);
        options.put(HiveCatalogFactoryOptions.HIVE_CONF_DIR.key(), CONF_DIR.getPath());
        options.put(HiveCatalogFactoryOptions.HADOOP_CONF_DIR.key(), hadoopConfDir);

        final Catalog actualCatalog =
                FactoryUtil.createCatalog(
                        catalogName, options, null, Thread.currentThread().getContextClassLoader());

        checkEquals(expectedCatalog, (HiveCatalog) actualCatalog);
        assertEquals(mapredVal, ((HiveCatalog) actualCatalog).getHiveConf().get(mapredKey));
    }

    @Test
    public void testCreateHiveCatalogWithIllegalHadoopConfDir() throws IOException {
        final String catalogName = "mycatalog";

        final String hadoopConfDir = tempFolder.newFolder().getAbsolutePath();

        try {
            final Map<String, String> options = new HashMap<>();
            options.put(
                    CommonCatalogOptions.CATALOG_TYPE.key(), HiveCatalogFactoryOptions.IDENTIFIER);
            options.put(HiveCatalogFactoryOptions.HIVE_CONF_DIR.key(), CONF_DIR.getPath());
            options.put(HiveCatalogFactoryOptions.HADOOP_CONF_DIR.key(), hadoopConfDir);

            final Catalog actualCatalog =
                    FactoryUtil.createCatalog(
                            catalogName,
                            options,
                            null,
                            Thread.currentThread().getContextClassLoader());
            Assert.fail();
        } catch (ValidationException e) {
        }
    }

    @Test
    public void testLoadHadoopConfigFromEnv() throws IOException {
        Map<String, String> customProps = new HashMap<>();
        String k1 = "what is connector?";
        String v1 = "Hive";
        final String catalogName = "HiveCatalog";

        // set HADOOP_CONF_DIR env
        final File hadoopConfDir = tempFolder.newFolder();
        final File hdfsSiteFile = new File(hadoopConfDir, "hdfs-site.xml");
        writeProperty(hdfsSiteFile, k1, v1);
        customProps.put(k1, v1);

        // add mapred-site file
        final File mapredSiteFile = new File(hadoopConfDir, "mapred-site.xml");
        k1 = "mapred.site.config.key";
        v1 = "mapred.site.config.val";
        writeProperty(mapredSiteFile, k1, v1);
        customProps.put(k1, v1);

        final Map<String, String> originalEnv = System.getenv();
        final Map<String, String> newEnv = new HashMap<>(originalEnv);
        newEnv.put("HADOOP_CONF_DIR", hadoopConfDir.getAbsolutePath());
        newEnv.remove("HADOOP_HOME");
        CommonTestUtils.setEnv(newEnv);

        // create HiveCatalog use the Hadoop Configuration
        final HiveConf hiveConf;
        try {
            final Map<String, String> options = new HashMap<>();
            options.put(
                    CommonCatalogOptions.CATALOG_TYPE.key(), HiveCatalogFactoryOptions.IDENTIFIER);
            options.put(HiveCatalogFactoryOptions.HIVE_CONF_DIR.key(), CONF_DIR.getPath());

            final HiveCatalog hiveCatalog =
                    (HiveCatalog)
                            FactoryUtil.createCatalog(
                                    catalogName,
                                    options,
                                    null,
                                    Thread.currentThread().getContextClassLoader());

            hiveConf = hiveCatalog.getHiveConf();
        } finally {
            // set the Env back
            CommonTestUtils.setEnv(originalEnv);
        }
        // validate the result
        for (String key : customProps.keySet()) {
            assertEquals(customProps.get(key), hiveConf.get(key, null));
        }
    }

    @Test
    public void testDisallowEmbedded() {
        expectedException.expect(ValidationException.class);

        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), HiveCatalogFactoryOptions.IDENTIFIER);

        FactoryUtil.createCatalog(
                "my_catalog", options, null, Thread.currentThread().getContextClassLoader());
    }

    @Test
    public void testCreateMultipleHiveCatalog() throws Exception {
        final Map<String, String> props1 = new HashMap<>();
        props1.put(CommonCatalogOptions.CATALOG_TYPE.key(), HiveCatalogFactoryOptions.IDENTIFIER);
        props1.put(
                HiveCatalogFactoryOptions.HIVE_CONF_DIR.key(),
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("test-multi-hive-conf1")
                        .getPath());

        final Map<String, String> props2 = new HashMap<>();
        props2.put(CommonCatalogOptions.CATALOG_TYPE.key(), HiveCatalogFactoryOptions.IDENTIFIER);
        props2.put(
                HiveCatalogFactoryOptions.HIVE_CONF_DIR.key(),
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("test-multi-hive-conf2")
                        .getPath());

        Callable<Catalog> callable1 =
                () ->
                        FactoryUtil.createCatalog(
                                "cat1",
                                props1,
                                null,
                                Thread.currentThread().getContextClassLoader());

        Callable<Catalog> callable2 =
                () ->
                        FactoryUtil.createCatalog(
                                "cat2",
                                props2,
                                null,
                                Thread.currentThread().getContextClassLoader());

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<Catalog> future1 = executorService.submit(callable1);
        Future<Catalog> future2 = executorService.submit(callable2);
        executorService.shutdown();

        HiveCatalog catalog1 = (HiveCatalog) future1.get();
        HiveCatalog catalog2 = (HiveCatalog) future2.get();

        // verify we read our own props
        assertEquals("val1", catalog1.getHiveConf().get("key"));
        assertNotNull(catalog1.getHiveConf().get("conf1", null));
        // verify we don't read props from other conf
        assertNull(catalog1.getHiveConf().get("conf2", null));

        // verify we read our own props
        assertEquals("val2", catalog2.getHiveConf().get("key"));
        assertNotNull(catalog2.getHiveConf().get("conf2", null));
        // verify we don't read props from other conf
        assertNull(catalog2.getHiveConf().get("conf1", null));
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
