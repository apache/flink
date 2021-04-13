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

package org.apache.flink.fs.gs.writer;

import org.apache.flink.fs.gs.GSFileSystemOptions;

/** Test recoverable writer. */
public class GSRecoverableWriterWithDefaultOptionsAndComplexCompositionTest
        extends GSRecoverableWriterWithOptionsTestBase {

    /**
     * Construct the test, using a data set count of 10 (i.e. >32) means that the composition will
     * be complex, i.e. performed in multiple steps. Since this uses the default temporary bucket,
     * there will be no copy step to create the final blob.
     */
    public GSRecoverableWriterWithDefaultOptionsAndComplexCompositionTest() {
        super(
                123,
                70,
                new GSFileSystemOptions(
                        "",
                        GSFileSystemOptions.DEFAULT_WRITER_TEMPORARY_OBJECT_PREFIX,
                        GSFileSystemOptions.DEFAULT_WRITER_CONTENT_TYPE,
                        0));
    }
}
