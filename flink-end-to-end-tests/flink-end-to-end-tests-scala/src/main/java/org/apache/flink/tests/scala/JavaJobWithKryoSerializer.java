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

package org.apache.flink.tests.scala;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Simple batch job in pure Java that uses a custom Kryo serializer. */
public class JavaJobWithKryoSerializer {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(
                PipelineOptions.SERIALIZATION_CONFIG.key(),
                "{org.apache.flink.tests.scala.NonPojo: "
                        + "{type: kryo, kryo-type: default, class: org.apache.flink.tests.scala.NonPojoSerializer}}");
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // we want to go through serialization to check for kryo issues
        env.disableOperatorChaining();

        env.fromData(new NonPojo()).map(x -> x);

        env.execute();
    }
}
