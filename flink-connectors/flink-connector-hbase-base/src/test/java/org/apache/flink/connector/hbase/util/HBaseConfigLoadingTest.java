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

package org.apache.flink.connector.hbase.util;

import org.apache.flink.core.testutils.CommonTestUtils;

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
 * Tests that validate the loading of the HBase configuration, relative to entries in the Flink
 * configuration and the environment variables.
 */
public class HBaseConfigLoadingTest {

    private static final String IN_HBASE_CONFIG_KEY = "hbase_conf_key";
    private static final String IN_HBASE_CONFIG_VALUE = "hbase_conf_value!";

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void loadFromClasspathByDefault() {
        org.apache.hadoop.conf.Configuration hbaseConf =
                HBaseConfigurationUtil.getHBaseConfiguration();

        assertEquals(IN_HBASE_CONFIG_VALUE, hbaseConf.get(IN_HBASE_CONFIG_KEY, null));
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

        final File hbaseConfDir = tempFolder.newFolder();

        final File hbaseHome = tempFolder.newFolder();

        final File hbaseHomeConf = new File(hbaseHome, "conf");

        assertTrue(hbaseHomeConf.mkdirs());

        final File file1 = new File(hbaseConfDir, "hbase-default.xml");
        final File file2 = new File(hbaseConfDir, "hbase-site.xml");
        final File file3 = new File(hbaseHomeConf, "hbase-default.xml");
        final File file4 = new File(hbaseHomeConf, "hbase-site.xml");

        printConfig(file1, k1, v1);
        printConfig(file2, k2, v2);
        printConfig(file3, k3, v3);
        printConfig(file4, k4, v4);

        final org.apache.hadoop.conf.Configuration hbaseConf;

        final Map<String, String> originalEnv = System.getenv();
        final Map<String, String> newEnv = new HashMap<>(originalEnv);
        newEnv.put("HBASE_CONF_DIR", hbaseConfDir.getAbsolutePath());
        newEnv.put("HBASE_HOME", hbaseHome.getAbsolutePath());
        try {
            CommonTestUtils.setEnv(newEnv);
            hbaseConf = HBaseConfigurationUtil.getHBaseConfiguration();
        } finally {
            CommonTestUtils.setEnv(originalEnv);
        }

        // contains extra entries
        assertEquals(v1, hbaseConf.get(k1, null));
        assertEquals(v2, hbaseConf.get(k2, null));
        assertEquals(v3, hbaseConf.get(k3, null));
        assertEquals(v4, hbaseConf.get(k4, null));

        // also contains classpath defaults
        assertEquals(IN_HBASE_CONFIG_VALUE, hbaseConf.get(IN_HBASE_CONFIG_KEY, null));
    }

    @Test
    public void loadOverlappingConfig() throws Exception {
        final String k1 = "key1";

        final String v1 = "from HBASE_HOME/conf";
        final String v2 = "from HBASE_CONF_DIR";

        final File hbaseHome = tempFolder.newFolder("hbaseHome");
        final File hbaseHomeConf = new File(hbaseHome, "conf");

        final File hbaseConfDir = tempFolder.newFolder("hbaseConfDir");

        assertTrue(hbaseHomeConf.mkdirs());
        final File file1 = new File(hbaseHomeConf, "hbase-site.xml");

        Map<String, String> properties1 = new HashMap<>();
        properties1.put(k1, v1);
        printConfigs(file1, properties1);

        // HBASE_CONF_DIR conf will override k1 with v2
        final File file2 = new File(hbaseConfDir, "hbase-site.xml");
        Map<String, String> properties2 = new HashMap<>();
        properties2.put(k1, v2);
        printConfigs(file2, properties2);

        final org.apache.hadoop.conf.Configuration hbaseConf;

        final Map<String, String> originalEnv = System.getenv();
        final Map<String, String> newEnv = new HashMap<>(originalEnv);
        newEnv.put("HBASE_CONF_DIR", hbaseConfDir.getAbsolutePath());
        newEnv.put("HBASE_HOME", hbaseHome.getAbsolutePath());
        try {
            CommonTestUtils.setEnv(newEnv);
            hbaseConf = HBaseConfigurationUtil.getHBaseConfiguration();
        } finally {
            CommonTestUtils.setEnv(originalEnv);
        }

        // contains extra entries
        assertEquals(v2, hbaseConf.get(k1, null));

        // also contains classpath defaults
        assertEquals(IN_HBASE_CONFIG_VALUE, hbaseConf.get(IN_HBASE_CONFIG_KEY, null));
    }

    private static void printConfig(File file, String key, String value) throws IOException {
        Map<String, String> map = new HashMap<>(1);
        map.put(key, value);
        printConfigs(file, map);
    }

    private static void printConfigs(File file, Map<String, String> properties) throws IOException {
        try (PrintStream out = new PrintStream(new FileOutputStream(file))) {
            out.println("<?xml version=\"1.0\"?>");
            out.println("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>");
            out.println("<configuration>");
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                out.println("\t<property>");
                out.println("\t\t<name>" + entry.getKey() + "</name>");
                out.println("\t\t<value>" + entry.getValue() + "</value>");
                out.println("\t</property>");
            }
            out.println("</configuration>");
        }
    }
}
