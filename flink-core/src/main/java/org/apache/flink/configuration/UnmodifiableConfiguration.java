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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Public;

import java.util.Properties;

/** Unmodifiable version of the Configuration class. */
@Public
public class UnmodifiableConfiguration extends Configuration {

    private static final long serialVersionUID = -8151292629158972280L;

    /**
     * Creates a new UnmodifiableConfiguration, which holds a copy of the given configuration that
     * cannot be altered.
     *
     * @param config The configuration with the original contents.
     */
    public UnmodifiableConfiguration(Configuration config) {
        super(config);
    }

    // --------------------------------------------------------------------------------------------
    //  All mutating methods must fail
    // --------------------------------------------------------------------------------------------

    @Override
    public void addAllToProperties(Properties props) {
        // override to make the UnmodifiableConfigurationTest happy
        super.addAllToProperties(props);
    }

    @Override
    public final void addAll(Configuration other) {
        error();
    }

    @Override
    public final void addAll(Configuration other, String prefix) {
        error();
    }

    @Override
    final <T> void setValueInternal(String key, T value) {
        error();
    }

    @Override
    public <T> boolean removeConfig(ConfigOption<T> configOption) {
        error();
        return false;
    }

    private void error() {
        throw new UnsupportedOperationException(
                "The configuration is unmodifiable; its contents cannot be changed.");
    }
}
