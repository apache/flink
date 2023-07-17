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

package org.apache.flink.table.planner.factories;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.ProcedureNotExistException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** A factory to create a catalog with some built-in procedures for testing purpose. */
public class TestProcedureCatalogFactory implements CatalogFactory {
    private static final String IDENTIFIER = "test_procedure_catalog";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();
        return new CatalogWithBuiltInProcedure(context.getName());
    }

    /** A catalog with some built-in procedures for testing purpose. */
    public static class CatalogWithBuiltInProcedure extends GenericInMemoryCatalog {

        private static final Map<ObjectPath, Procedure> PROCEDURE_MAP = new HashMap<>();

        static {
            PROCEDURE_MAP.put(
                    ObjectPath.fromString("system.generate_n"), new GenerateSequenceProcedure());
            PROCEDURE_MAP.put(ObjectPath.fromString("system.sum_n"), new SumProcedure());
            PROCEDURE_MAP.put(ObjectPath.fromString("system.get_year"), new GetYearProcedure());
            PROCEDURE_MAP.put(
                    ObjectPath.fromString("system.generate_user"), new GenerateUserProcedure());
        }

        public CatalogWithBuiltInProcedure(String name) {
            super(name);
        }

        @Override
        public List<String> listProcedures(String dbName)
                throws DatabaseNotExistException, CatalogException {
            if (!databaseExists(dbName)) {
                throw new DatabaseNotExistException(getName(), dbName);
            }
            return PROCEDURE_MAP.keySet().stream()
                    .filter(procedurePath -> procedurePath.getDatabaseName().equals(dbName))
                    .map(ObjectPath::getObjectName)
                    .collect(Collectors.toList());
        }

        @Override
        public Procedure getProcedure(ObjectPath procedurePath)
                throws ProcedureNotExistException, CatalogException {
            if (PROCEDURE_MAP.containsKey(procedurePath)) {
                return PROCEDURE_MAP.get(procedurePath);
            } else {
                throw new ProcedureNotExistException(getName(), procedurePath);
            }
        }
    }

    /** A procedure to a sequence from 0 to n for testing purpose. */
    public static class GenerateSequenceProcedure implements Procedure {
        public long[] call(ProcedureContext procedureContext, int n) throws Exception {
            return generate(procedureContext.getExecutionEnvironment(), n);
        }

        public long[] call(ProcedureContext procedureContext, int n, String runTimeMode)
                throws Exception {
            StreamExecutionEnvironment env = procedureContext.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.valueOf(runTimeMode));
            return generate(env, n);
        }

        private long[] generate(StreamExecutionEnvironment env, int n) throws Exception {
            env.setParallelism(1);
            long[] sequenceN = new long[n];
            int i = 0;
            try (CloseableIterator<Long> result = env.fromSequence(0, n - 1).executeAndCollect()) {
                while (result.hasNext()) {
                    sequenceN[i++] = result.next();
                }
            }
            return sequenceN;
        }
    }

    /** A procedure to sum decimal values for testing purpose. */
    public static class SumProcedure implements Procedure {
        public @DataTypeHint("ROW< sum_value decimal(10, 2), count INT >") Row[] call(
                ProcedureContext procedureContext,
                @DataTypeHint("DECIMAL(10, 2)") BigDecimal... inputs) {
            if (inputs.length == 0) {
                return new Row[] {Row.of(null, 0)};
            }
            int counts = inputs.length;
            BigDecimal result = inputs[0];
            for (int i = 1; i < inputs.length; i++) {
                result = result.add(inputs[i]);
            }
            return new Row[] {Row.of(result, counts)};
        }
    }

    /** A procedure to get year from the passed timestamp parameter for testing purpose. */
    public static class GetYearProcedure implements Procedure {
        public String[] call(ProcedureContext procedureContext, LocalDateTime... timestamps) {
            String[] results = new String[timestamps.length];
            for (int i = 0; i < results.length; i++) {
                results[i] = String.valueOf(timestamps[i].getYear());
            }
            return results;
        }
    }

    /** A procedure to generate a user according to the passed parameters for testing purpose. */
    public static class GenerateUserProcedure implements Procedure {
        public UserPojo[] call(ProcedureContext procedureContext, String name, Integer age) {
            return new UserPojo[] {new UserPojo(name, age)};
        }
    }

    /** A simple pojo class for testing purpose. */
    public static class UserPojo {
        private final String name;
        private final int age;

        public UserPojo(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UserPojo userPojo = (UserPojo) o;
            return age == userPojo.age && Objects.equals(name, userPojo.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age);
        }

        @Override
        public String toString() {
            return "UserPojo{" + "name='" + name + '\'' + ", age=" + age + '}';
        }
    }
}
