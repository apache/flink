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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;

import java.lang.reflect.Method;
import java.net.URL;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;
import static org.junit.Assert.assertNotEquals;

/**
 * This test validates that the RocksDB JNI library loading works properly in the presence of the
 * RocksDB code being loaded dynamically via reflection. That can happen when RocksDB is in the user
 * code JAR, or in certain test setups.
 */
public class RocksDbMultiClassLoaderTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testTwoSeparateClassLoaders() throws Exception {
        // collect the libraries / class folders with RocksDB related code: the state backend and
        // RocksDB itself
        final URL codePath1 =
                RocksDBStateBackend.class.getProtectionDomain().getCodeSource().getLocation();
        final URL codePath2 = RocksDB.class.getProtectionDomain().getCodeSource().getLocation();

        final ClassLoader parent = getClass().getClassLoader();
        final ClassLoader loader1 =
                FlinkUserCodeClassLoaders.childFirst(
                        new URL[] {codePath1, codePath2},
                        parent,
                        new String[0],
                        NOOP_EXCEPTION_HANDLER,
                        true);
        final ClassLoader loader2 =
                FlinkUserCodeClassLoaders.childFirst(
                        new URL[] {codePath1, codePath2},
                        parent,
                        new String[0],
                        NOOP_EXCEPTION_HANDLER,
                        true);

        final String className = RocksDBStateBackend.class.getName();

        final Class<?> clazz1 = Class.forName(className, false, loader1);
        final Class<?> clazz2 = Class.forName(className, false, loader2);
        assertNotEquals(
                "Test broken - the two reflectively loaded classes are equal", clazz1, clazz2);

        final Object instance1 =
                clazz1.getConstructor(String.class).newInstance(tmp.newFolder().toURI().toString());
        final Object instance2 =
                clazz2.getConstructor(String.class).newInstance(tmp.newFolder().toURI().toString());

        final String tempDir = tmp.newFolder().getAbsolutePath();

        final Method meth1 = clazz1.getDeclaredMethod("ensureRocksDBIsLoaded", String.class);
        final Method meth2 = clazz2.getDeclaredMethod("ensureRocksDBIsLoaded", String.class);
        meth1.setAccessible(true);
        meth2.setAccessible(true);

        // if all is well, these methods can both complete successfully
        meth1.invoke(instance1, tempDir);
        meth2.invoke(instance2, tempDir);
    }
}
