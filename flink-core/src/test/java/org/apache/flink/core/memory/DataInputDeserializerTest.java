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

package org.apache.flink.core.memory;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/** Test suite for the {@link DataInputDeserializer} class. */
public class DataInputDeserializerTest {

    @Test
    public void testAvailable() throws Exception {
        byte[] bytes;
        DataInputDeserializer dis;

        bytes = new byte[] {};
        dis = new DataInputDeserializer(bytes, 0, bytes.length);
        Assert.assertEquals(bytes.length, dis.available());

        bytes = new byte[] {1, 2, 3};
        dis = new DataInputDeserializer(bytes, 0, bytes.length);
        Assert.assertEquals(bytes.length, dis.available());

        dis.readByte();
        Assert.assertEquals(2, dis.available());
        dis.readByte();
        Assert.assertEquals(1, dis.available());
        dis.readByte();
        Assert.assertEquals(0, dis.available());

        try {
            dis.readByte();
            Assert.fail("Did not throw expected IOException");
        } catch (IOException e) {
            // ignore
        }
        Assert.assertEquals(0, dis.available());
    }
}
