/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.example.skew;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.GenericDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.io.output.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.skew.NativeGenIntInputFormat.ParetoIntGenerator;
import eu.stratosphere.pact.example.skew.NativeGenIntInputFormat.ZipfIntGenerator;

public class SkewRecordCount implements PlanAssembler, PlanAssemblerDescription {

	public static class FrequencyOutFormat extends TextOutputFormat<PactInteger, PactInteger> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public byte[] writeLine(KeyValuePair<PactInteger, PactInteger> pair) {
			String key = pair.getKey().toString();
			String value = pair.getValue().toString();
			String line = key + " " + value + "\n";
			return line.getBytes();
		}

	}

	public static class Identity extends MapStub<PactInteger, PactString, PactInteger, PactString> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void map(PactInteger key, PactString value, Collector<PactInteger, PactString> out) {

			out.collect(key, value);
			
		}

	}

	@SameKey
	public static class CountRecords extends ReduceStub<PactInteger, PactString, PactInteger, PactInteger> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void reduce(PactInteger key, Iterator<PactString> values, Collector<PactInteger, PactInteger> out) {
			int cnt = 0;
			while (values.hasNext()) {
				values.next();
				cnt++;
			}

			out.collect(key, new PactInteger(cnt));
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {

		int curArgIndex = 0;
		
		GenericDataSourceContract<PactInteger, PactString> input;
		
		String inputStr = (args.length > curArgIndex ? args[curArgIndex++] : "");
		if(inputStr.equals("myriad")) {

			////////////////////////////// MYRIAD
			
			input = new GenericDataSourceContract<PactInteger, PactString>(MyriadIntInputFormat.class, "Myriad Input");
			
			// scale
			float scale = (args.length > curArgIndex ? Float.parseFloat(args[curArgIndex++]) : 0.0f);
			if(scale <= 0.0f) throw new IllegalArgumentException("Myriad Arguments: scale binPath payLoad");
			input.getParameters().setFloat(MyriadGeneratorFixedLengthInputFormat.GENERATOR_SCALEFACTOR_PARAMETER_KEY, scale);
			
			// bin path
			String binPath = (args.length > curArgIndex ? args[curArgIndex++] : "");
			if(binPath.equals("")) throw new IllegalArgumentException("Myriad Arguments: scale binPath payLoad");
			input.getParameters().setString(MyriadGeneratorFixedLengthInputFormat.GENERATOR_PATH_PARAMETER_KEY, binPath);
			
			// payLoad
			int payLoad = (args.length > curArgIndex ? Integer.parseInt(args[curArgIndex++]) : -1);
			if(payLoad < 0) throw new IllegalArgumentException("Myriad Arguments: scale binPath payLoad");
			input.getParameters().setInteger(MyriadIntInputFormat.PAYLOAD_SIZE_PARAMETER_KEY, payLoad);
			
		} else if(inputStr.equals("nativeZipf")) {

			////////////////////////////// NATIVE ZIPF
			
			input = new GenericDataSourceContract<PactInteger, PactString>(NativeGenIntInputFormat.class, "Native Zipf Input");
			input.getParameters().setString(NativeGenIntInputFormat.DISTFUNC_PARAMETER_KEY, NativeGenIntInputFormat.ZIPF_DISTFUNC);
			
			// scale
			float scale = (args.length > curArgIndex ? Float.parseFloat(args[curArgIndex++]) : 0.0f);
			if(scale <= 0.0f) throw new IllegalArgumentException("Native Zipf Arguments: scale skew maxVal payLoad");
			input.getParameters().setFloat(NativeGenIntInputFormat.SCALEFACTOR_PARAMETER_KEY, scale);
			
			// skew
			float skew = (args.length > curArgIndex ? Float.parseFloat(args[curArgIndex++]) : 0.0f);
			if(skew <= 0.0f) throw new IllegalArgumentException("Native Zipf Arguments: scale skew maxVal payLoad");
			input.getParameters().setFloat(ZipfIntGenerator.SKEWFACTOR_PARAMETER_KEY, skew);
			
			// maxVal
			int maxVal = (args.length > curArgIndex ? Integer.parseInt(args[curArgIndex++]) : 0);
			if(maxVal <= 0) throw new IllegalArgumentException("Native Zipf Arguments: scale skew maxVal payLoad");
			input.getParameters().setInteger(ZipfIntGenerator.MAXVAL_PARAMETER_KEY, maxVal);
			
			// payLoad
			int payLoad = (args.length > curArgIndex ? Integer.parseInt(args[curArgIndex++]) : -1);
			if(payLoad < 0) throw new IllegalArgumentException("Native Zipf Arguments: scale skew maxVal payLoad");
			input.getParameters().setInteger(NativeGenIntInputFormat.PAYLOAD_SIZE_PARAMETER_KEY, payLoad);
			
		} else if(inputStr.equals("nativePareto")) {
			
			////////////////////////////// NATIVE PARETO
			
			input = new GenericDataSourceContract<PactInteger, PactString>(NativeGenIntInputFormat.class, "Native Pareto Input");
			input.getParameters().setString(NativeGenIntInputFormat.DISTFUNC_PARAMETER_KEY, NativeGenIntInputFormat.PARETO_DISTFUNC);
			
			// scale
			float scale = (args.length > curArgIndex ? Float.parseFloat(args[curArgIndex++]) : 0.0f);
			if(scale <= 0.0f) throw new IllegalArgumentException("Native Pareto Arguments: scale alpha x payLoad");
			input.getParameters().setFloat(NativeGenIntInputFormat.SCALEFACTOR_PARAMETER_KEY, scale);
			
			// skew
			float alpha = (args.length > curArgIndex ? Float.parseFloat(args[curArgIndex++]) : 0.0f);
			if(alpha <= 0.0f) throw new IllegalArgumentException("Native Pareto Arguments: scale alpha x payLoad");
			input.getParameters().setFloat(ParetoIntGenerator.ALPHA_PARAMETER_KEY, alpha);
			
			// maxVal
			float x = (args.length > curArgIndex ? Float.parseFloat(args[curArgIndex++]) : 0.0f);
			if(x <= 0) throw new IllegalArgumentException("Native Pareto Arguments: scale alpha x payLoad");
			input.getParameters().setFloat(ParetoIntGenerator.X_PARAMETER_KEY, x);
			
			// payLoad
			int payLoad = (args.length > curArgIndex ? Integer.parseInt(args[curArgIndex++]) : -1);
			if(payLoad < 0) throw new IllegalArgumentException("Native Pareto Arguments: scale alpha x payLoad");
			input.getParameters().setInteger(NativeGenIntInputFormat.PAYLOAD_SIZE_PARAMETER_KEY, payLoad);
			
		} else if(inputStr.equals("file")) {
			
			////////////////////////////// BINARY FILE
			
			// bin path
			String path = (args.length > curArgIndex ? args[curArgIndex++] : "");
			if(path.equals("")) throw new IllegalArgumentException("Binary File Arguments: path payLoad");
			
			input = new FileDataSourceContract<PactInteger, PactString>(BinaryFileIntInputFormat.class, path, "Binary File Input");
			
			// payLoad
			int payLoad = (args.length > curArgIndex ? Integer.parseInt(args[curArgIndex++]) : -1);
			if(payLoad < 0) throw new IllegalArgumentException("Binary File Arguments: path payLoad");
			input.getParameters().setInteger(BinaryFileIntInputFormat.PAYLOAD_SIZE_PARAMETER_KEY, payLoad);
			
		} else {
			throw new IllegalArgumentException("Specify input type!");
		}

		int numSendingTasks   = (args.length > curArgIndex ? Integer.parseInt(args[curArgIndex++]) : 1);
		int numReceivingTasks   = (args.length > curArgIndex ? Integer.parseInt(args[curArgIndex++]) : 1);
		String outPath    = (args.length > curArgIndex ? args[curArgIndex++] : "");
		
		input.setDegreeOfParallelism(numSendingTasks);

		MapContract<PactInteger, PactString, PactInteger, PactString> mapper = new MapContract<PactInteger, PactString, PactInteger, PactString>(
				Identity.class, "Identity Map");
		mapper.setDegreeOfParallelism(numSendingTasks);

		ReduceContract<PactInteger, PactString, PactInteger, PactInteger> reducer = new ReduceContract<PactInteger, PactString, PactInteger, PactInteger>(
				CountRecords.class, "Count Records");
		reducer.setDegreeOfParallelism(numReceivingTasks);

		FileDataSinkContract<PactInteger, PactInteger> out = new FileDataSinkContract<PactInteger, PactInteger>(
				FrequencyOutFormat.class, outPath, "Frequency Output");
		out.setDegreeOfParallelism(numReceivingTasks);

		out.setInput(reducer);
		reducer.setInput(mapper);
		mapper.setInput(input);

		return new Plan(out, "Skew Record Count");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "[ 'myriad' scale binPath | 'nativeZipf' scale skew maxVal | 'nativePareto' scale alpha x | 'file' path ] payLoad numSenders numReceivers outpath";
	}

}
