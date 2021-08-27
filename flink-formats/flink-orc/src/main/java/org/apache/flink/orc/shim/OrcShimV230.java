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

package org.apache.flink.orc.shim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;

/**
 * Shim orc for Hive version 2.1.0 and upper versions.
 *
 * <p>After hive 2.3 and later, the orc API is basically stable, so we can call it directly.
 *
 * <p>Since hive 2.2 not include orc classes, so we can use hive orc 2.3 to read from hive 2.2.
 */
public class OrcShimV230 extends OrcShimV210 {

    private static final long serialVersionUID = 1L;

    @Override
    protected Reader createReader(Path path, Configuration conf) throws IOException {
        return OrcFile.createReader(path, OrcFile.readerOptions(conf));
    }

    @Override
    protected RecordReader createRecordReader(Reader reader, Reader.Options options)
            throws IOException {
        return reader.rows(options);
    }

    @Override
    protected Reader.Options readOrcConf(Reader.Options options, Configuration conf) {
        return super.readOrcConf(options, conf)
                .tolerateMissingSchema(OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(conf));
    }
}
