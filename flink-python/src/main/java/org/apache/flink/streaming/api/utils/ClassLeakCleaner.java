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

package org.apache.flink.streaming.api.utils;

import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;

/** Utilities to clean up the leaking classes. */
public class ClassLeakCleaner {

    /** For each classloader we only need to execute the cleanup method once. */
    private static boolean leakedClassesCleanedUp = false;

    /** Clean up the soft references of the classes under the specified class loader. */
    public static synchronized void cleanUpLeakingClasses(ClassLoader classLoader)
            throws ReflectiveOperationException, SecurityException, ClassCastException {
        if (!leakedClassesCleanedUp) {
            // clear the soft references
            // see https://bugs.openjdk.java.net/browse/JDK-8199589 for more details
            Class<?> clazz = Class.forName("java.io.ObjectStreamClass$Caches");
            clearCache(clazz, "localDescs", classLoader);
            clearCache(clazz, "reflectors", classLoader);

            // It uses finalizers heavily in Netty which still holds the references to the user
            // class loader even after job finished.
            // so, trigger garbage collection explicitly to:
            // 1) trigger the execution of the `Finalizer`s of objects created by the finished jobs
            // of this TaskManager
            // 2) the references to the class loader will then be released and so the user class
            // loader could be garbage collected finally
            System.gc();
            leakedClassesCleanedUp = true;
        }
    }

    private static void clearCache(Class<?> target, String mapName, ClassLoader classLoader)
            throws ReflectiveOperationException, SecurityException, ClassCastException {
        Field f = target.getDeclaredField(mapName);
        f.setAccessible(true);
        Map<?, ?> map = (Map<?, ?>) f.get(null);
        Iterator<?> keys = map.keySet().iterator();
        while (keys.hasNext()) {
            Object key = keys.next();
            if (key instanceof Reference) {
                Object clazz = ((Reference<?>) key).get();
                if (clazz instanceof Class) {
                    ClassLoader cl = ((Class<?>) clazz).getClassLoader();
                    while (cl != null) {
                        if (cl == classLoader) {
                            keys.remove();
                            break;
                        }
                        cl = cl.getParent();
                    }
                }
            }
        }
    }
}
