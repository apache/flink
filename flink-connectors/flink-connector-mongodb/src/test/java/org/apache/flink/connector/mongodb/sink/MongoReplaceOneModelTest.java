/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.sink;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the MongoReplaceOneModel. */
public class MongoReplaceOneModelTest {

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        final Map<String, Object> filterDoc = ImmutableMap.of("filter_key", "filter_value");
        final Map<String, Object> replaceDoc = ImmutableMap.of("replace_key", "replace_value");
        final MongoReplaceOneModel mongoUpdateOneModel =
                new MongoReplaceOneModel(filterDoc, replaceDoc);
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        ObjectOutputStream outputStream = new ObjectOutputStream(bo);
        outputStream.writeObject(mongoUpdateOneModel);
        ByteArrayInputStream bi = new ByteArrayInputStream(bo.toByteArray());
        ObjectInputStream inputStream = new ObjectInputStream(bi);
        MongoReplaceOneModel read = (MongoReplaceOneModel) inputStream.readObject();

        assertThat(read).isEqualTo(mongoUpdateOneModel);
    }
}
