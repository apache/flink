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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.formats.hadoop.bulk.HadoopPathBasedBulkWriter;
import org.apache.flink.formats.hadoop.bulk.TestHadoopPathBasedBulkWriterFactory;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.util.FlinkUserCodeClassLoader;
import org.apache.flink.util.MutableURLClassLoader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests the behaviors of {@link HadoopPathBasedBulkFormatBuilder}. */
class HadoopPathBasedBulkFormatBuilderTest {

    /**
     * Tests if we could create {@link HadoopPathBasedBulkFormatBuilder} within user classloader. It
     * is mainly verify we have fixed the issue raised in
     * https://issues.apache.org/jira/browse/FLINK-19398.
     */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testCreatingBuildWithinUserClassLoader() throws Exception {
        ClassLoader appClassLoader = getClass().getClassLoader();
        assumeThat(appClassLoader).isInstanceOf(URLClassLoader.class);

        ClassLoader userClassLoader =
                new SpecifiedChildFirstUserClassLoader(
                        HadoopPathBasedBulkFormatBuilder.class.getName(),
                        appClassLoader,
                        ((URLClassLoader) appClassLoader).getURLs());

        Class<HadoopPathBasedBulkFormatBuilder> userHadoopFormatBuildClass =
                (Class<HadoopPathBasedBulkFormatBuilder>)
                        userClassLoader.loadClass(HadoopPathBasedBulkFormatBuilder.class.getName());
        Constructor<?> constructor =
                userHadoopFormatBuildClass.getConstructor(
                        Path.class,
                        HadoopPathBasedBulkWriter.Factory.class,
                        Configuration.class,
                        BucketAssigner.class);
        Object hadoopFormatBuilder =
                constructor.newInstance(
                        new Path("/tmp"),
                        new TestHadoopPathBasedBulkWriterFactory(),
                        new Configuration(),
                        new DateTimeBucketAssigner<>());

        Buckets<String, String> buckets =
                (Buckets<String, String>)
                        userHadoopFormatBuildClass
                                .getMethod("createBuckets", int.class)
                                .invoke(hadoopFormatBuilder, 0);
        assertThat(buckets).isNotNull();
    }

    private static class SpecifiedChildFirstUserClassLoader extends FlinkUserCodeClassLoader {

        private final String specifiedClassName;

        protected SpecifiedChildFirstUserClassLoader(
                String specifiedClassName, ClassLoader parent, URL[] urls) {
            super(urls, parent);
            this.specifiedClassName = specifiedClassName;
        }

        @Override
        protected Class<?> loadClassWithoutExceptionHandling(String name, boolean resolve)
                throws ClassNotFoundException {
            if (name.equals(specifiedClassName)) {
                return findClass(name);
            } else {
                return super.loadClassWithoutExceptionHandling(name, resolve);
            }
        }

        @Override
        public MutableURLClassLoader copy() {
            return new SpecifiedChildFirstUserClassLoader(
                    specifiedClassName, getParent(), getURLs());
        }
    }
}
