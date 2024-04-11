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

package org.apache.flink.connectors.hive;

import org.apache.flink.connectors.hive.write.HiveOutputFormatFactory;
import org.apache.flink.connectors.hive.write.HiveWriterFactory;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for HiveOutputFormatFactory. */
public class HiveOutputFormatFactoryTest {

    private static final String TEST_URI_SCHEME = "testscheme";
    private static final String TEST_URI_AUTHORITY = "test-uri-auth:8888";
    private static final String TEST_HMS_THRIFT_ENDPOINT = "thrift://METASTORE:9083";

    @Test
    public void testCreateOutputFormat() {
        SerDeInfo serDeInfo =
                new SerDeInfo("name", LazySimpleSerDe.class.getName(), Collections.emptyMap());
        JobConf jobConf = new JobConf();
        jobConf.set(HiveConf.ConfVars.METASTOREURIS.varname, TEST_HMS_THRIFT_ENDPOINT);
        HiveWriterFactory writerFactory =
                new HiveWriterFactory(
                        jobConf,
                        VerifyURIOutputFormat.class,
                        serDeInfo,
                        ResolvedSchema.of(Column.physical("x", DataTypes.INT())),
                        new String[0],
                        new Properties(),
                        HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion()),
                        false);
        HiveOutputFormatFactory factory = new HiveOutputFormatFactory(writerFactory);
        org.apache.flink.core.fs.Path path =
                new org.apache.flink.core.fs.Path(TEST_URI_SCHEME, TEST_URI_AUTHORITY, "/foo/path");
        org.apache.flink.connectors.hive.write.HiveOutputFormatFactory.HiveOutputFormat
                outputFormat = factory.createOutputFormat(path);
        LineageVertex lineageVertex = outputFormat.getLineageVertex();
        assertThat(lineageVertex.datasets()).hasSize(1);
        assertThat(lineageVertex.datasets().get(0).name())
                .isEqualTo("testscheme://test-uri-auth:8888/foo/path");
        assertThat(lineageVertex.datasets().get(0).namespace()).isEqualTo(TEST_HMS_THRIFT_ENDPOINT);
    }

    /** A HiveOutputFormat that verifies scheme and authority of the output path uri. */
    public static class VerifyURIOutputFormat implements HiveOutputFormat {

        @Override
        public FileSinkOperator.RecordWriter getHiveRecordWriter(
                JobConf jc,
                Path finalOutPath,
                Class valueClass,
                boolean isCompressed,
                Properties tableProperties,
                Progressable progress)
                throws IOException {
            URI uri = finalOutPath.toUri();
            assertThat(uri.getScheme()).isEqualTo(TEST_URI_SCHEME);
            assertThat(uri.getAuthority()).isEqualTo(TEST_URI_AUTHORITY);
            return null;
        }

        @Override
        public RecordWriter getRecordWriter(
                FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable)
                throws IOException {
            return null;
        }

        @Override
        public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {}
    }
}
