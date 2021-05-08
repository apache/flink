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

package org.apache.flink.docs.configuration;

import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Formatter;
import org.apache.flink.configuration.description.HtmlFormatter;
import org.apache.flink.docs.configuration.data.TestCommonOptions;
import org.apache.flink.util.FileUtils;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;

/** Tests for the {@link ConfigOptionsDocGenerator}. */
@SuppressWarnings("unused")
public class ConfigOptionsDocGeneratorTest {

    @ClassRule public static final TemporaryFolder TMP = new TemporaryFolder();

    static class TestConfigGroup {
        public static ConfigOption<Integer> firstOption =
                ConfigOptions.key("first.option.a")
                        .intType()
                        .defaultValue(2)
                        .withDescription("This is example description for the first option.");

        public static ConfigOption<String> secondOption =
                ConfigOptions.key("second.option.a")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("This is long example description for the second option.");
    }

    private enum TestEnum {
        VALUE_1,
        VALUE_2,
        VALUE_3
    }

    static class TypeTestConfigGroup {
        public static ConfigOption<TestEnum> enumOption =
                ConfigOptions.key("option.enum")
                        .enumType(TestEnum.class)
                        .defaultValue(TestEnum.VALUE_1)
                        .withDescription("Description");

        public static ConfigOption<List<TestEnum>> enumListOption =
                ConfigOptions.key("option.enum.list")
                        .enumType(TestEnum.class)
                        .asList()
                        .defaultValues(TestEnum.VALUE_1, TestEnum.VALUE_2)
                        .withDescription("Description");

        public static ConfigOption<MemorySize> memoryOption =
                ConfigOptions.key("option.memory")
                        .memoryType()
                        .defaultValue(new MemorySize(1024))
                        .withDescription("Description");

        public static ConfigOption<Map<String, String>> mapOption =
                ConfigOptions.key("option.map")
                        .mapType()
                        .defaultValue(Collections.singletonMap("key1", "value1"))
                        .withDescription("Description");

        public static ConfigOption<List<Map<String, String>>> mapListOption =
                ConfigOptions.key("option.map.list")
                        .mapType()
                        .asList()
                        .defaultValues(
                                Collections.singletonMap("key1", "value1"),
                                Collections.singletonMap("key2", "value2"))
                        .withDescription("Description");

        public static ConfigOption<Duration> durationOption =
                ConfigOptions.key("option.duration")
                        .durationType()
                        .defaultValue(Duration.ofMinutes(1))
                        .withDescription("Description");
    }

    @Test
    public void testCreatingTypes() {
        final String expectedTable =
                "<table class=\"configuration table table-bordered\">\n"
                        + "    <thead>\n"
                        + "        <tr>\n"
                        + "            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 10%\">Type</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 55%\">Description</th>\n"
                        + "        </tr>\n"
                        + "    </thead>\n"
                        + "    <tbody>\n"
                        + "        <tr>\n"
                        + "            <td><h5>option.duration</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">1 min</td>\n"
                        + "            <td>Duration</td>\n"
                        + "            <td>Description</td>\n"
                        + "        </tr>\n"
                        + "        <tr>\n"
                        + "            <td><h5>option.enum</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">VALUE_1</td>\n"
                        + "            <td><p>Enum</p>Possible values: [VALUE_1, VALUE_2, VALUE_3]</td>\n"
                        + "            <td>Description</td>\n"
                        + "        </tr>\n"
                        + "        <tr>\n"
                        + "            <td><h5>option.enum.list</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">VALUE_1;<wbr>VALUE_2</td>\n"
                        + "            <td><p>List&lt;Enum&gt;</p>Possible values: [VALUE_1, VALUE_2, VALUE_3]</td>\n"
                        + "            <td>Description</td>\n"
                        + "        </tr>\n"
                        + "        <tr>\n"
                        + "            <td><h5>option.map</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">key1:value1</td>\n"
                        + "            <td>Map</td>\n"
                        + "            <td>Description</td>\n"
                        + "        </tr>\n"
                        + "        <tr>\n"
                        + "            <td><h5>option.map.list</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">key1:value1;<wbr>key2:value2</td>\n"
                        + "            <td>List&lt;Map&gt;</td>\n"
                        + "            <td>Description</td>\n"
                        + "        </tr>\n"
                        + "        <tr>\n"
                        + "            <td><h5>option.memory</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">1 kb</td>\n"
                        + "            <td>MemorySize</td>\n"
                        + "            <td>Description</td>\n"
                        + "        </tr>\n"
                        + "    </tbody>\n"
                        + "</table>\n";
        final String htmlTable =
                ConfigOptionsDocGenerator.generateTablesForClass(TypeTestConfigGroup.class)
                        .get(0)
                        .f1;

        assertEquals(expectedTable, htmlTable);
    }

    @Test
    public void testCreatingDescription() {
        final String expectedTable =
                "<table class=\"configuration table table-bordered\">\n"
                        + "    <thead>\n"
                        + "        <tr>\n"
                        + "            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 10%\">Type</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 55%\">Description</th>\n"
                        + "        </tr>\n"
                        + "    </thead>\n"
                        + "    <tbody>\n"
                        + "        <tr>\n"
                        + "            <td><h5>first.option.a</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">2</td>\n"
                        + "            <td>Integer</td>\n"
                        + "            <td>This is example description for the first option.</td>\n"
                        + "        </tr>\n"
                        + "        <tr>\n"
                        + "            <td><h5>second.option.a</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">(none)</td>\n"
                        + "            <td>String</td>\n"
                        + "            <td>This is long example description for the second option.</td>\n"
                        + "        </tr>\n"
                        + "    </tbody>\n"
                        + "</table>\n";
        final String htmlTable =
                ConfigOptionsDocGenerator.generateTablesForClass(TestConfigGroup.class).get(0).f1;

        assertEquals(expectedTable, htmlTable);
    }

    @ConfigGroups(
            groups = {
                @ConfigGroup(name = "group1", keyPrefix = "a.b"),
                @ConfigGroup(name = "group2", keyPrefix = "a.b.c.d")
            })
    static class TestConfigPrefix {
        // should end up in the default group
        public static ConfigOption<Integer> option1 =
                ConfigOptions.key("a.option").intType().defaultValue(2);

        // should end up in group1, perfect key-prefix match
        public static ConfigOption<String> option2 =
                ConfigOptions.key("a.b.option").stringType().noDefaultValue();

        // should end up in group1, full key-prefix match
        public static ConfigOption<Integer> option3 =
                ConfigOptions.key("a.b.c.option").intType().defaultValue(2);

        // should end up in group1, full key-prefix match for group 1, partial match for group 2
        // checks that the generator remembers the last encountered root node
        public static ConfigOption<Integer> option4 =
                ConfigOptions.key("a.b.c.e.option").intType().defaultValue(2);

        // should end up in the default group, since no group exists with prefix "a.c"
        // checks that the generator does not ignore components (like ignoring "c" to find a match
        // "a.b")
        public static ConfigOption<String> option5 =
                ConfigOptions.key("a.c.b.option").stringType().noDefaultValue();

        // should end up in group2, full key-prefix match for group 2
        // checks that the longest matching group is assigned
        public static ConfigOption<Integer> option6 =
                ConfigOptions.key("a.b.c.d.option").intType().defaultValue(2);
    }

    @Test
    public void testLongestPrefixMatching() {
        final List<Tuple2<ConfigGroup, String>> tables =
                ConfigOptionsDocGenerator.generateTablesForClass(TestConfigPrefix.class);

        assertEquals(3, tables.size());
        final Map<String, String> tablesConverted = new HashMap<>(tables.size());
        for (final Tuple2<ConfigGroup, String> table : tables) {
            tablesConverted.put(table.f0 != null ? table.f0.name() : "default", table.f1);
        }

        assertThat(tablesConverted.get("group1"), containsString("a.b.option"));
        assertThat(tablesConverted.get("group1"), containsString("a.b.c.option"));
        assertThat(tablesConverted.get("group1"), containsString("a.b.c.e.option"));
        assertThat(tablesConverted.get("group2"), containsString("a.b.c.d.option"));
        assertThat(tablesConverted.get("group1"), not(containsString("a.b.c.d.option")));
        assertThat(tablesConverted.get("default"), containsString("a.option"));
        assertThat(tablesConverted.get("default"), containsString("a.c.b.option"));
    }

    @ConfigGroups(
            groups = {
                @ConfigGroup(name = "firstGroup", keyPrefix = "first"),
                @ConfigGroup(name = "secondGroup", keyPrefix = "second")
            })
    static class TestConfigMultipleSubGroup {
        public static ConfigOption<Integer> firstOption =
                ConfigOptions.key("first.option.a")
                        .intType()
                        .defaultValue(2)
                        .withDescription("This is example description for the first option.");

        public static ConfigOption<String> secondOption =
                ConfigOptions.key("second.option.a")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("This is long example description for the second option.");

        public static ConfigOption<Integer> thirdOption =
                ConfigOptions.key("third.option.a")
                        .intType()
                        .defaultValue(2)
                        .withDescription("This is example description for the third option.");

        public static ConfigOption<String> fourthOption =
                ConfigOptions.key("fourth.option.a")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("This is long example description for the fourth option.");
    }

    @Test
    public void testCreatingMultipleGroups() {
        final List<Tuple2<ConfigGroup, String>> tables =
                ConfigOptionsDocGenerator.generateTablesForClass(TestConfigMultipleSubGroup.class);

        assertEquals(tables.size(), 3);
        final HashMap<String, String> tablesConverted = new HashMap<>();
        for (Tuple2<ConfigGroup, String> table : tables) {
            tablesConverted.put(table.f0 != null ? table.f0.name() : "default", table.f1);
        }

        assertEquals(
                "<table class=\"configuration table table-bordered\">\n"
                        + "    <thead>\n"
                        + "        <tr>\n"
                        + "            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 10%\">Type</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 55%\">Description</th>\n"
                        + "        </tr>\n"
                        + "    </thead>\n"
                        + "    <tbody>\n"
                        + "        <tr>\n"
                        + "            <td><h5>first.option.a</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">2</td>\n"
                        + "            <td>Integer</td>\n"
                        + "            <td>This is example description for the first option.</td>\n"
                        + "        </tr>\n"
                        + "    </tbody>\n"
                        + "</table>\n",
                tablesConverted.get("firstGroup"));
        assertEquals(
                "<table class=\"configuration table table-bordered\">\n"
                        + "    <thead>\n"
                        + "        <tr>\n"
                        + "            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 10%\">Type</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 55%\">Description</th>\n"
                        + "        </tr>\n"
                        + "    </thead>\n"
                        + "    <tbody>\n"
                        + "        <tr>\n"
                        + "            <td><h5>second.option.a</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">(none)</td>\n"
                        + "            <td>String</td>\n"
                        + "            <td>This is long example description for the second option.</td>\n"
                        + "        </tr>\n"
                        + "    </tbody>\n"
                        + "</table>\n",
                tablesConverted.get("secondGroup"));
        assertEquals(
                "<table class=\"configuration table table-bordered\">\n"
                        + "    <thead>\n"
                        + "        <tr>\n"
                        + "            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 10%\">Type</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 55%\">Description</th>\n"
                        + "        </tr>\n"
                        + "    </thead>\n"
                        + "    <tbody>\n"
                        + "        <tr>\n"
                        + "            <td><h5>fourth.option.a</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">(none)</td>\n"
                        + "            <td>String</td>\n"
                        + "            <td>This is long example description for the fourth option.</td>\n"
                        + "        </tr>\n"
                        + "        <tr>\n"
                        + "            <td><h5>third.option.a</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">2</td>\n"
                        + "            <td>Integer</td>\n"
                        + "            <td>This is example description for the third option.</td>\n"
                        + "        </tr>\n"
                        + "    </tbody>\n"
                        + "</table>\n",
                tablesConverted.get("default"));
    }

    static class TestConfigGroupWithOverriddenDefault {
        @Documentation.OverrideDefault("default_1")
        public static ConfigOption<Integer> firstOption =
                ConfigOptions.key("first.option.a")
                        .intType()
                        .defaultValue(2)
                        .withDescription("This is example description for the first option.");

        @Documentation.OverrideDefault("default_2")
        public static ConfigOption<String> secondOption =
                ConfigOptions.key("second.option.a")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("This is long example description for the second option.");
    }

    @Test
    public void testOverrideDefault() {
        final String expectedTable =
                "<table class=\"configuration table table-bordered\">\n"
                        + "    <thead>\n"
                        + "        <tr>\n"
                        + "            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 10%\">Type</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 55%\">Description</th>\n"
                        + "        </tr>\n"
                        + "    </thead>\n"
                        + "    <tbody>\n"
                        + "        <tr>\n"
                        + "            <td><h5>first.option.a</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">default_1</td>\n"
                        + "            <td>Integer</td>\n"
                        + "            <td>This is example description for the first option.</td>\n"
                        + "        </tr>\n"
                        + "        <tr>\n"
                        + "            <td><h5>second.option.a</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">default_2</td>\n"
                        + "            <td>String</td>\n"
                        + "            <td>This is long example description for the second option.</td>\n"
                        + "        </tr>\n"
                        + "    </tbody>\n"
                        + "</table>\n";
        final String htmlTable =
                ConfigOptionsDocGenerator.generateTablesForClass(
                                TestConfigGroupWithOverriddenDefault.class)
                        .get(0)
                        .f1;

        assertEquals(expectedTable, htmlTable);
    }

    @Test
    public void testSections() throws IOException, ClassNotFoundException {
        final String projectRootDir = getProjectRootDir();
        final String outputDirectory = TMP.newFolder().getAbsolutePath();

        final OptionsClassLocation[] locations =
                new OptionsClassLocation[] {
                    new OptionsClassLocation(
                            "flink-docs", TestCommonOptions.class.getPackage().getName())
                };

        ConfigOptionsDocGenerator.generateCommonSection(
                projectRootDir, outputDirectory, locations, "src/test/java");
        Formatter formatter = new HtmlFormatter();

        String expected1 =
                "<table class=\"configuration table table-bordered\">\n"
                        + "    <thead>\n"
                        + "        <tr>\n"
                        + "            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 10%\">Type</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 55%\">Description</th>\n"
                        + "        </tr>\n"
                        + "    </thead>\n"
                        + "    <tbody>\n"
                        + "        <tr>\n"
                        + "            <td><h5>"
                        + TestCommonOptions.COMMON_POSITIONED_OPTION.key()
                        + "</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">"
                        + TestCommonOptions.COMMON_POSITIONED_OPTION.defaultValue()
                        + "</td>\n"
                        + "            <td>Integer</td>\n"
                        + "            <td>"
                        + formatter.format(TestCommonOptions.COMMON_POSITIONED_OPTION.description())
                        + "</td>\n"
                        + "        </tr>\n"
                        + "        <tr>\n"
                        + "            <td><h5>"
                        + TestCommonOptions.COMMON_OPTION.key()
                        + "</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">"
                        + TestCommonOptions.COMMON_OPTION.defaultValue()
                        + "</td>\n"
                        + "            <td>Integer</td>\n"
                        + "            <td>"
                        + formatter.format(TestCommonOptions.COMMON_OPTION.description())
                        + "</td>\n"
                        + "        </tr>\n"
                        + "    </tbody>\n"
                        + "</table>\n";

        String expected2 =
                "<table class=\"configuration table table-bordered\">\n"
                        + "    <thead>\n"
                        + "        <tr>\n"
                        + "            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 10%\">Type</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 55%\">Description</th>\n"
                        + "        </tr>\n"
                        + "    </thead>\n"
                        + "    <tbody>\n"
                        + "        <tr>\n"
                        + "            <td><h5>"
                        + TestCommonOptions.COMMON_OPTION.key()
                        + "</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">"
                        + TestCommonOptions.COMMON_OPTION.defaultValue()
                        + "</td>\n"
                        + "            <td>Integer</td>\n"
                        + "            <td>"
                        + formatter.format(TestCommonOptions.COMMON_OPTION.description())
                        + "</td>\n"
                        + "        </tr>\n"
                        + "    </tbody>\n"
                        + "</table>\n";

        final String fileName1 =
                ConfigOptionsDocGenerator.getSectionFileName(TestCommonOptions.SECTION_1);
        final String fileName2 =
                ConfigOptionsDocGenerator.getSectionFileName(TestCommonOptions.SECTION_2);

        final String output1 =
                FileUtils.readFile(
                        new File(outputDirectory, fileName1), StandardCharsets.UTF_8.name());
        final String output2 =
                FileUtils.readFile(
                        new File(outputDirectory, fileName2), StandardCharsets.UTF_8.name());

        assertEquals(expected1, output1);
        assertEquals(expected2, output2);
    }

    static class TestConfigGroupWithExclusion {
        public static ConfigOption<Integer> firstOption =
                ConfigOptions.key("first.option.a")
                        .intType()
                        .defaultValue(2)
                        .withDescription("This is example description for the first option.");

        @Documentation.ExcludeFromDocumentation
        public static ConfigOption<String> excludedOption =
                ConfigOptions.key("excluded.option.a")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("This should not be documented.");
    }

    /**
     * Tests that {@link ConfigOption} annotated with {@link Documentation.ExcludeFromDocumentation}
     * are not documented.
     */
    @Test
    public void testConfigOptionExclusion() {
        final String expectedTable =
                "<table class=\"configuration table table-bordered\">\n"
                        + "    <thead>\n"
                        + "        <tr>\n"
                        + "            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 10%\">Type</th>\n"
                        + "            <th class=\"text-left\" style=\"width: 55%\">Description</th>\n"
                        + "        </tr>\n"
                        + "    </thead>\n"
                        + "    <tbody>\n"
                        + "        <tr>\n"
                        + "            <td><h5>first.option.a</h5></td>\n"
                        + "            <td style=\"word-wrap: break-word;\">2</td>\n"
                        + "            <td>Integer</td>\n"
                        + "            <td>This is example description for the first option.</td>\n"
                        + "        </tr>\n"
                        + "    </tbody>\n"
                        + "</table>\n";
        final String htmlTable =
                ConfigOptionsDocGenerator.generateTablesForClass(TestConfigGroupWithExclusion.class)
                        .get(0)
                        .f1;

        assertEquals(expectedTable, htmlTable);
    }

    @ConfigGroups(groups = {@ConfigGroup(name = "firstGroup", keyPrefix = "first")})
    static class EmptyConfigOptions {}

    @Test
    public void testClassWithoutOptionsIsIgnored() {
        assertThat(
                ConfigOptionsDocGenerator.generateTablesForClass(EmptyConfigOptions.class),
                empty());
    }

    @Test
    public void testSnakeCaseConversion() {
        assertEquals("rocks_options", ConfigOptionsDocGenerator.toSnakeCase("RocksOptions"));
        assertEquals("rocksdb_options", ConfigOptionsDocGenerator.toSnakeCase("RocksDBOptions"));
        assertEquals("db_options", ConfigOptionsDocGenerator.toSnakeCase("DBOptions"));
    }

    static String getProjectRootDir() {
        final String dirFromProperty = System.getProperty("rootDir");
        if (dirFromProperty != null) {
            return dirFromProperty;
        }

        // to make this work in the IDE use a default fallback
        final String currentDir = System.getProperty("user.dir");
        return new File(currentDir).getParent();
    }
}
