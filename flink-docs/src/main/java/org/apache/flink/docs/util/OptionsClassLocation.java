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

package org.apache.flink.docs.util;

import org.apache.flink.configuration.ConfigOption;

/** Simple descriptor for the location of a class containing {@link ConfigOption ConfigOptions}. */
public class OptionsClassLocation {
    private final String module;
    private final String pckg;

    public OptionsClassLocation(String module, String pckg) {
        this.module = module;
        this.pckg = pckg;
    }

    public String getModule() {
        return module;
    }

    public String getPackage() {
        return pckg;
    }

    @Override
    public String toString() {
        return module + "#" + pckg;
    }
}
