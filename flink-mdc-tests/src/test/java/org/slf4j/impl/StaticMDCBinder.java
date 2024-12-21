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

package org.slf4j.impl;

import org.slf4j.helpers.BasicMDCAdapter;
import org.slf4j.spi.MDCAdapter;

/**
 * MDC binder for SLF4J 1.x that ensures a correct MDC is used, as we can't get rid of
 * log4j-slf4j-impl that comes from parent POM.
 *
 * <p>After upgrade to SLF4J2 can be replaced with service provider configuration.
 */
public final class StaticMDCBinder {

    public static final StaticMDCBinder SINGLETON = new StaticMDCBinder();

    private StaticMDCBinder() {}

    public static StaticMDCBinder getSingleton() {
        return SINGLETON;
    }

    public MDCAdapter getMDCA() {
        return new BasicMDCAdapter();
    }

    public String getMDCAdapterClassStr() {
        return BasicMDCAdapter.class.getName();
    }
}
