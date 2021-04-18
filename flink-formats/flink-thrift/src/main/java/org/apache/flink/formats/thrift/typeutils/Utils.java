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

package org.apache.flink.formats.thrift.typeutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Utility methods. */
public class Utils {

    public static List<String> getClassAndTypeArguments(String genericClassName) {
        genericClassName = genericClassName.trim();
        List<String> classNames = new ArrayList<>();

        if (!genericClassName.contains("<")) {
            classNames.add(genericClassName);
            return classNames;
        }

        String[] splits = genericClassName.split(">")[0].split("<");
        String mainClassName = splits[0];
        String[] typeArgs = splits[1].split(",");
        classNames.add(mainClassName);
        classNames.addAll(Arrays.asList(typeArgs));
        return classNames;
    }

    public static boolean isAssignableFrom(Class superClass, Class clazz) throws IOException {
        return isAssignableFrom(superClass, clazz.getName());
    }

    public static boolean isAssignableFrom(Class superClass, String className) throws IOException {
        Class clazz;
        try {
            clazz = Class.forName(className);
            return superClass.isAssignableFrom(clazz);
        } catch (ClassNotFoundException e) {
            List<String> decomposed = getClassAndTypeArguments(className);
            try {
                clazz = Class.forName(decomposed.get(0));
                return superClass.isAssignableFrom(clazz);
            } catch (ClassNotFoundException e2) {
                throw new IOException(e2);
            }
        }
    }
}
