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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.common.Converter;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** MiniCluster-based integration tests CSV data format. */
@ExtendWith({TestLoggerExtension.class})
public class DataStreamCsvITCase {

    private static final CsvMapper CSV_MAPPER = JacksonMapperFactory.createCsvMapper();

    private static final int PARALLELISM = 4;

    @TempDir File outDir;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    // ------------------------------------------------------------------------
    //  test data
    // ------------------------------------------------------------------------

    private static final String[] CSV_LINES =
            new String[] {
                "Berlin,52.5167,13.3833,Germany,DE,Berlin,primary,3644826",
                "San Francisco,37.7562,-122.443,United States,US,California,,3592294",
                "Beijing,39.905,116.3914,China,CN,Beijing,primary,19433000"
            };

    private static final String[] CSV_LINES_PIPE_SEPARATED =
            new String[] {
                "Berlin|52.5167|13.3833|Germany|DE|Berlin|primary|3644826",
                "San Francisco|37.7562|-122.443|United States|US|California||3592294",
                "Beijing|39.905|116.3914|China|CN|Beijing|primary|19433000"
            };

    private static final String[] CSV_LINES_MALFORMED =
            new String[] {
                "Berlin,52.5167,13.3833,Germany,DE,Berlin,primary,3644826",
                "San Francisco,MALFORMED,3592294",
                "Beijing,39.905,116.3914,China,CN,Beijing,primary,19433000"
            };

    static final CityPojo[] POJOS =
            new CityPojo[] {
                new CityPojo(
                        "Berlin",
                        new BigDecimal("52.5167"),
                        new BigDecimal("13.3833"),
                        "Germany",
                        "DE",
                        "Berlin",
                        "primary",
                        3644826L),
                new CityPojo(
                        "San Francisco",
                        new BigDecimal("37.7562"),
                        new BigDecimal("-122.443"),
                        "United States",
                        "US",
                        "California",
                        "",
                        3592294L),
                new CityPojo(
                        "Beijing",
                        new BigDecimal("39.905"),
                        new BigDecimal("116.3914"),
                        "China",
                        "CN",
                        "Beijing",
                        "primary",
                        19433000L)
            };

    // ------------------------------------------------------------------------
    //  test cases
    // ------------------------------------------------------------------------
    @Test
    public void testCsvReaderFormatFromPojo() throws Exception {
        writeFile(outDir, "data.csv", CSV_LINES);

        final CsvReaderFormat<CityPojo> csvFormat = CsvReaderFormat.forPojo(CityPojo.class);
        final List<CityPojo> result = initializeSourceAndReadData(outDir, csvFormat);

        assertThat(Arrays.asList(POJOS)).isEqualTo(result);
    }

    @Test
    public void testCsvReaderFormatFromSchema() throws Exception {
        writeFile(outDir, "data.csv", CSV_LINES_PIPE_SEPARATED);

        final CsvReaderFormat<CityPojo> csvFormat =
                CsvReaderFormat.forSchema(
                        () -> CSV_MAPPER,
                        mapper ->
                                mapper.schemaFor(CityPojo.class)
                                        .withoutQuoteChar()
                                        .withColumnSeparator('|'),
                        TypeInformation.of(CityPojo.class));
        final List<CityPojo> result = initializeSourceAndReadData(outDir, csvFormat);

        assertThat(Arrays.asList(POJOS)).isEqualTo(result);
    }

    @Test
    public void testCsvReaderFormatMalformed() throws Exception {
        writeFile(outDir, "data.csv", CSV_LINES_MALFORMED);

        final CsvReaderFormat<CityPojo> csvFormat =
                CsvReaderFormat.forPojo(CityPojo.class).withIgnoreParseErrors();
        final List<CityPojo> result = initializeSourceAndReadData(outDir, csvFormat);

        List<CityPojo> expected = new ArrayList<>();
        expected.add(POJOS[0]);
        expected.add(POJOS[2]);

        assertThat(expected).isEqualTo(result);
    }

    @Test
    public void testCustomBulkWriter() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        // fromCollection is not bounded, using fromSequence instead
        final List<CityPojo> pojosList = Arrays.asList(POJOS); // needs to be Serializable
        final DataStream<Integer> sequence =
                env.fromSequence(0, POJOS.length - 1).map(Long::intValue);
        final DataStream<CityPojo> stream = sequence.map(pojosList::get).returns(CityPojo.class);

        FileSink<CityPojo> sink =
                FileSink.forBulkFormat(new Path(outDir.toURI()), factoryForPojo(CityPojo.class))
                        .withBucketAssigner(new BasePathBucketAssigner<>())
                        .build();

        stream.sinkTo(sink);
        env.execute();

        String[] result = getResultsFromSinkFiles(outDir);

        assertThat(result).containsExactlyInAnyOrder(CSV_LINES);
    }

    @NotNull
    private String[] getResultsFromSinkFiles(File outDir) throws IOException {
        final Map<File, String> contents = getFileContentByPath(outDir);

        List<String> resultList =
                contents.entrySet().stream()
                        .flatMap(e -> Arrays.stream(e.getValue().split("\n")))
                        .collect(Collectors.toList());

        String[] result = resultList.toArray(new String[0]);
        return result;
    }

    private static <T> BulkWriter.Factory<T> factoryForPojo(Class<T> pojoClass) {
        final Converter<T, T, Void> converter = (value, context) -> value;
        final CsvSchema schema = CSV_MAPPER.schemaFor(pojoClass).withoutQuoteChar();
        return (out) -> new CsvBulkWriter<>(CSV_MAPPER, schema, converter, null, out);
    }

    private static Map<File, String> getFileContentByPath(File directory) throws IOException {
        Map<File, String> contents = new HashMap<>(4);

        final Collection<File> filesInBucket = FileUtils.listFiles(directory, null, true);
        for (File file : filesInBucket) {
            contents.put(file, FileUtils.readFileToString(file));
        }
        return contents;
    }

    /** Test pojo describing a city. */
    @JsonPropertyOrder({
        "city",
        "lat",
        "lng",
        "country",
        "iso2",
        "adminName",
        "capital",
        "population"
    })
    public static class CityPojo implements Serializable {
        public String city;
        public BigDecimal lat;
        public BigDecimal lng;
        public String country;
        public String iso2;
        public String adminName;
        public String capital;
        public long population;

        public CityPojo() {}

        public CityPojo(
                String city,
                BigDecimal lat,
                BigDecimal lng,
                String country,
                String iso2,
                String adminName,
                String capital,
                long population) {
            this.city = city;
            this.lat = lat;
            this.lng = lng;
            this.country = country;
            this.iso2 = iso2;
            this.adminName = adminName;
            this.capital = capital;
            this.population = population;
        }

        @Override
        public String toString() {
            return "CitiesPojo{"
                    + "city='"
                    + city
                    + '\''
                    + ", lat="
                    + lat
                    + ", lng="
                    + lng
                    + ", country='"
                    + country
                    + '\''
                    + ", iso2='"
                    + iso2
                    + '\''
                    + ", adminName='"
                    + adminName
                    + '\''
                    + ", capital='"
                    + capital
                    + '\''
                    + ", population="
                    + population
                    + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CityPojo that = (CityPojo) o;
            return population == that.population
                    && Objects.equals(city, that.city)
                    && Objects.equals(lat, that.lat)
                    && Objects.equals(lng, that.lng)
                    && Objects.equals(country, that.country)
                    && Objects.equals(iso2, that.iso2)
                    && Objects.equals(adminName, that.adminName)
                    && Objects.equals(capital, that.capital);
        }

        @Override
        public int hashCode() {
            return Objects.hash(city, lat, lng, country, iso2, adminName, capital, population);
        }
    }

    private static <T> List<T> initializeSourceAndReadData(File testDir, StreamFormat<T> csvFormat)
            throws Exception {
        final FileSource<T> source =
                FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(testDir)).build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        final DataStream<T> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        return getResultsFromStream(stream);
    }

    @NotNull
    private static <T> List<T> getResultsFromStream(DataStream<T> stream) throws Exception {
        final ClientAndIterator<T> client =
                DataStreamUtils.collectWithClient(stream, "Bounded Results Fetch");

        final List<T> result = new ArrayList<>();
        while (client.iterator.hasNext()) {
            T next = client.iterator.next();
            result.add(next);
        }
        return result;
    }

    // ------------------------------------------------------------------------
    //  Write data utils
    // ------------------------------------------------------------------------

    private static void writeFile(File testDir, String fileName, String[] lines)
            throws IOException {
        final File file = new File(testDir, fileName);
        writeFileAtomically(file, lines);
    }

    private static void writeFileAtomically(final File file, final String[] lines)
            throws IOException {
        writeFileAtomically(file, lines, (v) -> v);
    }

    private static void writeFileAtomically(
            final File file,
            final String[] lines,
            final FunctionWithException<OutputStream, OutputStream, IOException>
                    streamEncoderFactory)
            throws IOException {

        final File stagingFile =
                new File(file.getParentFile(), ".tmp-" + UUID.randomUUID().toString());

        try (final FileOutputStream fileOut = new FileOutputStream(stagingFile);
                final OutputStream out = streamEncoderFactory.apply(fileOut);
                final OutputStreamWriter encoder =
                        new OutputStreamWriter(out, StandardCharsets.UTF_8);
                final PrintWriter writer = new PrintWriter(encoder)) {

            for (String line : lines) {
                writer.println(line);
            }
        }

        final File parent = file.getParentFile();

        assertThat(parent.mkdirs() || parent.exists()).isTrue();
        assertThat(stagingFile.renameTo(file)).isTrue();
    }
}
