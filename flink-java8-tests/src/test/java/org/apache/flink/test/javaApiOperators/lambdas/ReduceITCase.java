/**
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

package org.apache.flink.test.javaApiOperators.lambdas;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

@SuppressWarnings("serial")
public class ReduceITCase extends JavaProgramTestBase {

	private static final String EXPECTED_RESULT = "1,1,0,Hallo,1\n" +
			"2,3,2,Hallo Welt wie,1\n" +
			"2,2,1,Hallo Welt,2\n" +
			"3,9,0,P-),2\n" +
			"3,6,5,BCD,3\n" +
			"4,17,0,P-),1\n" +
			"4,17,0,P-),2\n" +
			"5,11,10,GHI,1\n" +
			"5,29,0,P-),2\n" +
			"5,25,0,P-),3\n";
	
	public static DataSet<Tuple5<Integer, Long, Integer, String, Long>> get5TupleDataSet(ExecutionEnvironment env) {

		List<Tuple5<Integer, Long, Integer, String, Long>> data = new ArrayList<Tuple5<Integer, Long, Integer, String, Long>>();
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(1,1l,0,"Hallo",1l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(2,2l,1,"Hallo Welt",2l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(2,3l,2,"Hallo Welt wie",1l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(3,4l,3,"Hallo Welt wie gehts?",2l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(3,5l,4,"ABC",2l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(3,6l,5,"BCD",3l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(4,7l,6,"CDE",2l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(4,8l,7,"DEF",1l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(4,9l,8,"EFG",1l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(4,10l,9,"FGH",2l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(5,11l,10,"GHI",1l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(5,12l,11,"HIJ",3l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(5,13l,12,"IJK",3l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(5,14l,13,"JKL",2l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(5,15l,14,"KLM",2l));

		Collections.shuffle(data);

		TupleTypeInfo<Tuple5<Integer, Long,  Integer, String, Long>> type = new
				TupleTypeInfo<Tuple5<Integer, Long,  Integer, String, Long>>(
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO
		);

		return env.fromCollection(data, type);
	}
	
	private String resultPath;
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = get5TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> reduceDs = ds
				.groupBy(4, 0)
				.reduce((in1, in2) -> {
					Tuple5<Integer, Long, Integer, String, Long> out = new Tuple5<Integer, Long, Integer, String, Long>();
					out.setFields(in1.f0, in1.f1 + in2.f1, 0, "P-)", in1.f4);
					return out;
				});

		reduceDs.writeAsCsv(resultPath);
		env.execute();
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED_RESULT, resultPath);
	}
}
