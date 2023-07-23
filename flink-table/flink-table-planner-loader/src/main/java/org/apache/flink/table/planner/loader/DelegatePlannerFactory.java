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

package org.apache.flink.table.planner.loader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.delegation.ParserFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.net.URL;
import java.security.ProtectionDomain;
import java.util.HashSet;
import java.util.Set;

/** Delegate of {@link PlannerFactory}. */
@Internal
public class DelegatePlannerFactory extends BaseDelegateFactory<PlannerFactory>
        implements PlannerFactory {

    private final Set<SqlDialect> seenDialect = new HashSet<>();

    public DelegatePlannerFactory() {
        super(PlannerModule.getInstance().loadPlannerFactory());
        seenDialect.add(SqlDialect.DEFAULT);
    }

    @Override
    public Planner create(Context context) {
        SqlDialect currentDialect = context.getTableConfig().getSqlDialect();
        if (!seenDialect.contains(currentDialect)) {
            URL dialectJarUrl =
                    tryGetJarURLForDialect(PlannerFactory.class.getClassLoader(), currentDialect);
            PlannerModule.getInstance().addUrlToClassLoader(dialectJarUrl);
        }
        return delegate.create(context);
    }

    private URL tryGetJarURLForDialect(ClassLoader classLoader, SqlDialect sqlDialect) {
        ParserFactory parserFactory =
                FactoryUtil.discoverFactory(
                        classLoader, ParserFactory.class, sqlDialect.name().toLowerCase());
        // get the domain of this class
        ProtectionDomain protectionDomain = parserFactory.getClass().getProtectionDomain();
        // get the location for the jar which contains this class
        return protectionDomain.getCodeSource().getLocation();
    }
}
