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

package org.apache.flink.connectors.hive;

import com.klarna.hiverunner.builder.HiveShellBuilder;
import com.klarna.hiverunner.config.HiveRunnerConfig;

import java.lang.reflect.Method;

/** Shim for hive runner 4.x. */
public class HiveRunnerShimV4 implements HiveRunnerShim {

    @Override
    public void setCommandShellEmulation(HiveShellBuilder builder, HiveRunnerConfig config)
            throws Exception {
        Method method = config.getClass().getDeclaredMethod("getCommandShellEmulator");
        Object emulator = method.invoke(config);
        Class emulatorClz = Class.forName("com.klarna.hiverunner.sql.cli.CommandShellEmulator");
        method = builder.getClass().getDeclaredMethod("setCommandShellEmulation", emulatorClz);
        method.invoke(builder, emulator);
    }
}
