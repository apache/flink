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

package org.apache.flink.testutils;

import java.util.Set;

/**
 * Utility classloader used in tests that allows simulating {@link ClassNotFoundException}s for
 * specific classes.
 */
public class ArtificialCNFExceptionThrowingClassLoader extends ClassLoader {

    private final Set<String> cnfThrowingClassnames;

    public ArtificialCNFExceptionThrowingClassLoader(
            ClassLoader parent, Set<String> cnfThrowingClassnames) {
        super(parent);
        this.cnfThrowingClassnames = cnfThrowingClassnames;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (cnfThrowingClassnames.contains(name)) {
            throw new ClassNotFoundException();
        } else {
            return super.loadClass(name, resolve);
        }
    }
}
