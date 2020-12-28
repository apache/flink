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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.CheckedSupplier;

import akka.actor.ActorSystem;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for {@link BootstrapToolsTest}. */
public class BootstrapToolsTest extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(BootstrapToolsTest.class);

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testSubstituteConfigKey() {
        String deprecatedKey1 = "deprecated-key";
        String deprecatedKey2 = "another-out_of-date_key";
        String deprecatedKey3 = "yet-one-more";

        String designatedKey1 = "newkey1";
        String designatedKey2 = "newKey2";
        String designatedKey3 = "newKey3";

        String value1 = "value1";
        String value2Designated = "designated-value2";
        String value2Deprecated = "deprecated-value2";

        // config contains only deprecated key 1, and for key 2 both deprecated and designated
        Configuration cfg = new Configuration();
        cfg.setString(deprecatedKey1, value1);
        cfg.setString(deprecatedKey2, value2Deprecated);
        cfg.setString(designatedKey2, value2Designated);

        BootstrapTools.substituteDeprecatedConfigKey(cfg, deprecatedKey1, designatedKey1);
        BootstrapTools.substituteDeprecatedConfigKey(cfg, deprecatedKey2, designatedKey2);
        BootstrapTools.substituteDeprecatedConfigKey(cfg, deprecatedKey3, designatedKey3);

        // value 1 should be set to designated
        assertEquals(value1, cfg.getString(designatedKey1, null));

        // value 2 should not have been set, since it had a value already
        assertEquals(value2Designated, cfg.getString(designatedKey2, null));

        // nothing should be in there for key 3
        assertNull(cfg.getString(designatedKey3, null));
        assertNull(cfg.getString(deprecatedKey3, null));
    }

    @Test
    public void testSubstituteConfigKeyPrefix() {
        String deprecatedPrefix1 = "deprecated-prefix";
        String deprecatedPrefix2 = "-prefix-2";
        String deprecatedPrefix3 = "prefix-3";

        String designatedPrefix1 = "p1";
        String designatedPrefix2 = "ppp";
        String designatedPrefix3 = "zzz";

        String depr1 = deprecatedPrefix1 + "var";
        String depr2 = deprecatedPrefix2 + "env";
        String depr3 = deprecatedPrefix2 + "x";

        String desig1 = designatedPrefix1 + "var";
        String desig2 = designatedPrefix2 + "env";
        String desig3 = designatedPrefix2 + "x";

        String val1 = "1";
        String val2 = "2";
        String val3Depr = "3-";
        String val3Desig = "3+";

        // config contains only deprecated key 1, and for key 2 both deprecated and designated
        Configuration cfg = new Configuration();
        cfg.setString(depr1, val1);
        cfg.setString(depr2, val2);
        cfg.setString(depr3, val3Depr);
        cfg.setString(desig3, val3Desig);

        BootstrapTools.substituteDeprecatedConfigPrefix(cfg, deprecatedPrefix1, designatedPrefix1);
        BootstrapTools.substituteDeprecatedConfigPrefix(cfg, deprecatedPrefix2, designatedPrefix2);
        BootstrapTools.substituteDeprecatedConfigPrefix(cfg, deprecatedPrefix3, designatedPrefix3);

        assertEquals(val1, cfg.getString(desig1, null));
        assertEquals(val2, cfg.getString(desig2, null));
        assertEquals(val3Desig, cfg.getString(desig3, null));

        // check that nothing with prefix 3 is contained
        for (String key : cfg.keySet()) {
            assertFalse(key.startsWith(designatedPrefix3));
            assertFalse(key.startsWith(deprecatedPrefix3));
        }
    }

    @Test
    public void testGetTaskManagerShellCommand() {
        final Configuration cfg = new Configuration();
        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                new TaskExecutorProcessSpec(
                        new CPUResource(1.0),
                        new MemorySize(0), // frameworkHeapSize
                        new MemorySize(0), // frameworkOffHeapSize
                        new MemorySize(111), // taskHeapSize
                        new MemorySize(0), // taskOffHeapSize
                        new MemorySize(222), // networkMemSize
                        new MemorySize(0), // managedMemorySize
                        new MemorySize(333), // jvmMetaspaceSize
                        new MemorySize(0)); // jvmOverheadSize
        final ContaineredTaskManagerParameters containeredParams =
                new ContaineredTaskManagerParameters(
                        taskExecutorProcessSpec, new HashMap<String, String>());

        // no logging, with/out krb5
        final String java = "$JAVA_HOME/bin/java";
        final String jvmmem =
                "-Xmx111 -Xms111 -XX:MaxDirectMemorySize=222 -XX:MaxMetaspaceSize=333";
        final String jvmOpts = "-Djvm"; // if set
        final String tmJvmOpts = "-DtmJvm"; // if set
        final String logfile = "-Dlog.file=./logs/taskmanager.log"; // if set
        final String logback = "-Dlogback.configurationFile=file:./conf/logback.xml"; // if set
        final String log4j =
                "-Dlog4j.configuration=file:./conf/log4j.properties"
                        + " -Dlog4j.configurationFile=file:./conf/log4j.properties"; // if set
        final String mainClass = "org.apache.flink.runtime.clusterframework.BootstrapToolsTest";
        final String dynamicConfigs =
                TaskExecutorProcessUtils.generateDynamicConfigsStr(taskExecutorProcessSpec).trim();
        final String basicArgs = "--configDir ./conf";
        final String mainArgs = "-Djobmanager.rpc.address=host1 -Dkey.a=v1";
        final String args = dynamicConfigs + " " + basicArgs + " " + mainArgs;
        final String redirects = "1> ./logs/taskmanager.out 2> ./logs/taskmanager.err";

        assertEquals(
                java
                        + " "
                        + jvmmem
                        + ""
                        + // jvmOpts
                        ""
                        + // logging
                        " "
                        + mainClass
                        + " "
                        + dynamicConfigs
                        + " "
                        + basicArgs
                        + " "
                        + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        false,
                        false,
                        false,
                        this.getClass(),
                        ""));

        assertEquals(
                java + " " + jvmmem + "" + // jvmOpts
                        "" + // logging
                        " " + mainClass + " " + args + " " + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        false,
                        false,
                        false,
                        this.getClass(),
                        mainArgs));

        final String krb5 = "-Djava.security.krb5.conf=krb5.conf";
        assertEquals(
                java + " " + jvmmem + " " + krb5 + // jvmOpts
                        "" + // logging
                        " " + mainClass + " " + args + " " + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        false,
                        false,
                        true,
                        this.getClass(),
                        mainArgs));

        // logback only, with/out krb5
        assertEquals(
                java + " " + jvmmem + "" + // jvmOpts
                        " " + logfile + " " + logback + " " + mainClass + " " + args + " "
                        + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        false,
                        false,
                        this.getClass(),
                        mainArgs));

        assertEquals(
                java + " " + jvmmem + " " + krb5 + // jvmOpts
                        " " + logfile + " " + logback + " " + mainClass + " " + args + " "
                        + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        false,
                        true,
                        this.getClass(),
                        mainArgs));

        // log4j, with/out krb5
        assertEquals(
                java + " " + jvmmem + "" + // jvmOpts
                        " " + logfile + " " + log4j + " " + mainClass + " " + args + " "
                        + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        false,
                        true,
                        false,
                        this.getClass(),
                        mainArgs));

        assertEquals(
                java + " " + jvmmem + " " + krb5 + // jvmOpts
                        " " + logfile + " " + log4j + " " + mainClass + " " + args + " "
                        + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        false,
                        true,
                        true,
                        this.getClass(),
                        mainArgs));

        // logback + log4j, with/out krb5
        assertEquals(
                java + " " + jvmmem + "" + // jvmOpts
                        " " + logfile + " " + logback + " " + log4j + " " + mainClass + " " + args
                        + " " + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        false,
                        this.getClass(),
                        mainArgs));

        assertEquals(
                java + " " + jvmmem + " " + krb5 + // jvmOpts
                        " " + logfile + " " + logback + " " + log4j + " " + mainClass + " " + args
                        + " " + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        true,
                        this.getClass(),
                        mainArgs));

        // logback + log4j, with/out krb5, different JVM opts
        cfg.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
        assertEquals(
                java + " " + jvmmem + " " + jvmOpts + " " + logfile + " " + logback + " " + log4j
                        + " " + mainClass + " " + args + " " + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        false,
                        this.getClass(),
                        mainArgs));

        assertEquals(
                java + " " + jvmmem + " " + jvmOpts + " " + krb5 + // jvmOpts
                        " " + logfile + " " + logback + " " + log4j + " " + mainClass + " " + args
                        + " " + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        true,
                        this.getClass(),
                        mainArgs));

        // logback + log4j, with/out krb5, different JVM opts
        cfg.setString(CoreOptions.FLINK_TM_JVM_OPTIONS, tmJvmOpts);
        assertEquals(
                java + " " + jvmmem + " " + jvmOpts + " " + tmJvmOpts + " " + logfile + " "
                        + logback + " " + log4j + " " + mainClass + " " + args + " " + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        false,
                        this.getClass(),
                        mainArgs));

        assertEquals(
                java + " " + jvmmem + " " + jvmOpts + " " + tmJvmOpts + " " + krb5 + // jvmOpts
                        " " + logfile + " " + logback + " " + log4j + " " + mainClass + " " + args
                        + " " + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        true,
                        this.getClass(),
                        mainArgs));

        // now try some configurations with different yarn.container-start-command-template

        cfg.setString(
                ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
                "%java% 1 %jvmmem% 2 %jvmopts% 3 %logging% 4 %class% 5 %args% 6 %redirects%");
        assertEquals(
                java + " 1 " + jvmmem + " 2 " + jvmOpts + " " + tmJvmOpts + " " + krb5 + // jvmOpts
                        " 3 " + logfile + " " + logback + " " + log4j + " 4 " + mainClass + " 5 "
                        + args + " 6 " + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        true,
                        this.getClass(),
                        mainArgs));

        cfg.setString(
                ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
                "%java% %logging% %jvmopts% %jvmmem% %class% %args% %redirects%");
        assertEquals(
                java + " " + logfile + " " + logback + " " + log4j + " " + jvmOpts + " " + tmJvmOpts
                        + " " + krb5 + // jvmOpts
                        " " + jvmmem + " " + mainClass + " " + args + " " + redirects,
                BootstrapTools.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        true,
                        this.getClass(),
                        mainArgs));
    }

    @Test
    public void testUpdateTmpDirectoriesInConfiguration() {
        Configuration config = new Configuration();

        // test that default value is taken
        BootstrapTools.updateTmpDirectoriesInConfiguration(config, "default/directory/path");
        assertEquals(config.getString(CoreOptions.TMP_DIRS), "default/directory/path");

        // test that we ignore default value is value is set before
        BootstrapTools.updateTmpDirectoriesInConfiguration(config, "not/default/directory/path");
        assertEquals(config.getString(CoreOptions.TMP_DIRS), "default/directory/path");

        // test that empty value is not a magic string
        config.setString(CoreOptions.TMP_DIRS, "");
        BootstrapTools.updateTmpDirectoriesInConfiguration(config, "some/new/path");
        assertEquals(config.getString(CoreOptions.TMP_DIRS), "");
    }

    @Test
    public void testShouldNotUpdateTmpDirectoriesInConfigurationIfNoValueConfigured() {
        Configuration config = new Configuration();
        BootstrapTools.updateTmpDirectoriesInConfiguration(config, null);
        assertEquals(config.getString(CoreOptions.TMP_DIRS), CoreOptions.TMP_DIRS.defaultValue());
    }

    /**
     * Tests that we can concurrently create two {@link ActorSystem} without port conflicts. This
     * effectively tests that we don't open a socket to check for a ports availability. See
     * FLINK-10580 for more details.
     */
    @Test
    public void testConcurrentActorSystemCreation() throws Exception {
        final int concurrentCreations = 10;
        final ExecutorService executorService = Executors.newFixedThreadPool(concurrentCreations);
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(concurrentCreations);

        try {
            final List<CompletableFuture<Void>> actorSystemFutures =
                    IntStream.range(0, concurrentCreations)
                            .mapToObj(
                                    ignored ->
                                            CompletableFuture.supplyAsync(
                                                    CheckedSupplier.unchecked(
                                                            () -> {
                                                                cyclicBarrier.await();

                                                                return BootstrapTools
                                                                        .startRemoteActorSystem(
                                                                                new Configuration(),
                                                                                "localhost",
                                                                                "0",
                                                                                LOG);
                                                            }),
                                                    executorService))
                            .map(
                                    // terminate ActorSystems
                                    actorSystemFuture ->
                                            actorSystemFuture.thenCompose(
                                                    AkkaUtils::terminateActorSystem))
                            .collect(Collectors.toList());

            FutureUtils.completeAll(actorSystemFutures).get();
        } finally {
            ExecutorUtils.gracefulShutdown(10000L, TimeUnit.MILLISECONDS, executorService);
        }
    }

    /**
     * Tests that the {@link ActorSystem} fails with an expressive exception if it cannot be
     * instantiated due to an occupied port.
     */
    @Test
    public void testActorSystemInstantiationFailureWhenPortOccupied() throws Exception {
        final ServerSocket portOccupier = new ServerSocket(0, 10, InetAddress.getByName("0.0.0.0"));

        try {
            final int port = portOccupier.getLocalPort();
            BootstrapTools.startRemoteActorSystem(
                    new Configuration(), "0.0.0.0", String.valueOf(port), LOG);
            fail("Expected to fail with a BindException");
        } catch (Exception e) {
            assertThat(ExceptionUtils.findThrowable(e, BindException.class).isPresent(), is(true));
        } finally {
            portOccupier.close();
        }
    }

    @Test
    public void testGetDynamicPropertiesAsString() {
        final Configuration baseConfig = new Configuration();
        baseConfig.setString("key.a", "a");
        baseConfig.setString("key.b", "b1");

        final Configuration targetConfig = new Configuration();
        targetConfig.setString("key.b", "b2");
        targetConfig.setString("key.c", "c");

        final String dynamicProperties =
                BootstrapTools.getDynamicPropertiesAsString(baseConfig, targetConfig);
        if (OperatingSystem.isWindows()) {
            assertEquals("-Dkey.b=\"b2\" -Dkey.c=\"c\"", dynamicProperties);
        } else {
            assertEquals("-Dkey.b='b2' -Dkey.c='c'", dynamicProperties);
        }
    }

    @Test
    public void testEscapeDynamicPropertyValueWithSingleQuote() {
        final String value1 = "#a,b&c^d*e@f(g!h";
        assertEquals("'" + value1 + "'", BootstrapTools.escapeWithSingleQuote(value1));

        final String value2 = "'foobar";
        assertEquals("''\\''foobar'", BootstrapTools.escapeWithSingleQuote(value2));

        final String value3 = "foo''bar";
        assertEquals("'foo'\\'''\\''bar'", BootstrapTools.escapeWithSingleQuote(value3));

        final String value4 = "'foo' 'bar'";
        assertEquals("''\\''foo'\\'' '\\''bar'\\'''", BootstrapTools.escapeWithSingleQuote(value4));
    }

    @Test
    public void testEscapeDynamicPropertyValueWithDoubleQuote() {
        final String value1 = "#a,b&c^d*e@f(g!h";
        assertEquals("\"#a,b&c\"^^\"d*e@f(g!h\"", BootstrapTools.escapeWithDoubleQuote(value1));

        final String value2 = "foo\"bar'";
        assertEquals("\"foo\\\"bar'\"", BootstrapTools.escapeWithDoubleQuote(value2));

        final String value3 = "\"foo\" \"bar\"";
        assertEquals("\"\\\"foo\\\" \\\"bar\\\"\"", BootstrapTools.escapeWithDoubleQuote(value3));
    }

    @Test
    public void testGetEnvironmentVariables() {
        Configuration testConf = new Configuration();
        testConf.setString("containerized.master.env.LD_LIBRARY_PATH", "/usr/lib/native");

        Map<String, String> res =
                ConfigurationUtils.getPrefixedKeyValuePairs("containerized.master.env.", testConf);

        Assert.assertEquals(1, res.size());
        Map.Entry<String, String> entry = res.entrySet().iterator().next();
        Assert.assertEquals("LD_LIBRARY_PATH", entry.getKey());
        Assert.assertEquals("/usr/lib/native", entry.getValue());
    }

    @Test
    public void testGetEnvironmentVariablesErroneous() {
        Configuration testConf = new Configuration();
        testConf.setString("containerized.master.env.", "/usr/lib/native");

        Map<String, String> res =
                ConfigurationUtils.getPrefixedKeyValuePairs("containerized.master.env.", testConf);

        Assert.assertEquals(0, res.size());
    }

    @Test
    public void testWriteConfigurationAndReload() throws IOException {
        final File flinkConfDir = temporaryFolder.newFolder().getAbsoluteFile();
        final Configuration flinkConfig = new Configuration();

        final ConfigOption<List<String>> listStringConfigOption =
                ConfigOptions.key("test-list-string-key").stringType().asList().noDefaultValue();
        final List<String> list =
                Arrays.asList("A,B,C,D", "A'B'C'D", "A;BCD", "AB\"C\"D", "AB'\"D:B");
        flinkConfig.set(listStringConfigOption, list);
        assertThat(flinkConfig.get(listStringConfigOption), containsInAnyOrder(list.toArray()));

        final ConfigOption<List<Duration>> listDurationConfigOption =
                ConfigOptions.key("test-list-duration-key")
                        .durationType()
                        .asList()
                        .noDefaultValue();
        final List<Duration> durationList =
                Arrays.asList(Duration.ofSeconds(3), Duration.ofMinutes(1));
        flinkConfig.set(listDurationConfigOption, durationList);
        assertThat(
                flinkConfig.get(listDurationConfigOption),
                containsInAnyOrder(durationList.toArray()));

        final ConfigOption<Map<String, String>> mapConfigOption =
                ConfigOptions.key("test-map-key").mapType().noDefaultValue();
        final Map<String, String> map = new HashMap<>();
        map.put("key1", "A,B,C,D");
        map.put("key2", "A;BCD");
        map.put("key3", "A'B'C'D");
        map.put("key4", "AB\"C\"D");
        map.put("key5", "AB'\"D:B");
        flinkConfig.set(mapConfigOption, map);
        assertThat(
                flinkConfig.get(mapConfigOption).entrySet(),
                containsInAnyOrder(map.entrySet().toArray()));

        final ConfigOption<Duration> durationConfigOption =
                ConfigOptions.key("test-duration-key").durationType().noDefaultValue();
        final Duration duration = Duration.ofMillis(3000);
        flinkConfig.set(durationConfigOption, duration);
        assertEquals(duration, flinkConfig.get(durationConfigOption));

        BootstrapTools.writeConfiguration(flinkConfig, new File(flinkConfDir, FLINK_CONF_FILENAME));
        final Configuration loadedFlinkConfig =
                GlobalConfiguration.loadConfiguration(flinkConfDir.getAbsolutePath());
        assertThat(
                loadedFlinkConfig.get(listStringConfigOption), containsInAnyOrder(list.toArray()));
        assertThat(
                loadedFlinkConfig.get(listDurationConfigOption),
                containsInAnyOrder(durationList.toArray()));
        assertThat(
                loadedFlinkConfig.get(mapConfigOption).entrySet(),
                containsInAnyOrder(map.entrySet().toArray()));
        assertEquals(duration, loadedFlinkConfig.get(durationConfigOption));
    }
}
