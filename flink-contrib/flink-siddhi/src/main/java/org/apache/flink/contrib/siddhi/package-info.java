/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * <h1> Features </h1>
 * <ul>
 * <li>
 * Integrate Siddhi CEP as an  stream operator (i.e. `TupleStreamSiddhiOperator`), supporting rich CEP features like
 * <ul>
 * <li>Filter</li>
 * <li>Join</li>
 * <li>Aggregation</li>
 * <li>Group by</li>
 * <li>Having</li>
 * <li>Window</li>
 * <li>Conditions and Expressions</li>
 * <li>Pattern processing</li>
 * <li>Sequence processing</li>
 * <li>Event Tables</li>
 * <li>...</li>
 * </ul>
 * </li>
 * <li>
 * Provide easy-to-use Siddhi CEP API to integrate Flink DataStream API (See `SiddhiCEP` and `SiddhiStream`)
 * <ul>
 * <li>Register Flink DataStream associating native type information with Siddhi Stream Schema, supporting POJO,Tuple, Primitive Type, etc.</li>
 * <li>Connect with single or multiple Flink DataStreams with Siddhi CEP Execution Plan</li>
 * <li>Return output stream as DataStream with type intelligently inferred from Siddhi Stream Schema</li>
 * </ul>
 * </li>
 * <li>
 * Integrate siddhi runtime state management with Flink state (See `AbstractSiddhiOperator`)
 * </li>
 * <li>
 * Support siddhi plugin management to extend CEP functions. (See `SiddhiCEP#registerExtension`)
 * </li>
 * </ul>
 * <p/>
 * <h1>Example</h1>
 * <pre>
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * SiddhiCEP cep = SiddhiCEP.getSiddhiEnvironment(env);
 *
 * cep.registerExtension("custom:plus",CustomPlusFunctionExtension.class);
 *
 * cep.registerStream("inputStream1", input1, "id", "name", "price","timestamp");
 * cep.registerStream("inputStream2", input2, "id", "name", "price","timestamp");
 *
 * DataStream&lt;Tuple4&lt;Integer,String,Integer,String&gt;&gt; output = cep
 * 	.from("inputStream1").union("inputStream2")
 * 	.sql(
 * 		"from every s1 = inputStream1[id == 2] "
 * 		 + " -> s2 = inputStream2[id == 3] "
 * 		 + "select s1.id as id_1, s1.name as name_1, s2.id as id_2, s2.name as name_2 "
 * 		 + "insert into outputStream"
 * 	)
 * 	.returns("outputStream");
 *
 * env.execute();
 * </pre>
 *
 * @see <a href="https://github.com/wso2/siddhi">https://github.com/wso2/siddhi</a>
 */
package org.apache.flink.contrib.siddhi;
