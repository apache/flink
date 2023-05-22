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

package org.apache.flink.api.common.io;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

public class InputStreamFSInputWrapperTest {

    @Test
    public void testClose() throws Exception {
        final AtomicBoolean closeCalled = new AtomicBoolean(false);
        InputStream mockedInputStream =
                new InputStream() {
                    @Override
                    public int read() {
                        return 0;
                    }

                    @Override
                    public void close() throws IOException {
                        closeCalled.set(true);
                        super.close();
                    }
                };
        InputStreamFSInputWrapper wrapper = new InputStreamFSInputWrapper(mockedInputStream);
        wrapper.close();
        assertThat(closeCalled).isTrue();
    }
}
