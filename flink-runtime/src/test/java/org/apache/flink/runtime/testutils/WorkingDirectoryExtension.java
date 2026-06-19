/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.testutils;

import org.apache.flink.core.testutils.CustomExtension;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

/** Extension to generate {@link WorkingDirectory}. */
public class WorkingDirectoryExtension implements CustomExtension {
    private final Supplier<File> tmpDirectorySupplier;

    private int counter = 0;

    public WorkingDirectoryExtension(Supplier<File> tmpDirectorySupplier) {
        // We must use supplier because @TempDir has not yet been injected when constructing this
        // extension.
        this.tmpDirectorySupplier = tmpDirectorySupplier;
    }

    public WorkingDirectory createNewWorkingDirectory() throws IOException {
        return WorkingDirectory.create(
                new File(tmpDirectorySupplier.get(), "working_directory_" + counter++));
    }
}
