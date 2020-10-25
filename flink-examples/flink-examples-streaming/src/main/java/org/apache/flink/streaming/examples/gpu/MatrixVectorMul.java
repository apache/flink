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

package org.apache.flink.streaming.examples.gpu;

import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Preconditions;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.jcublas.JCublas;
import jcuda.runtime.JCuda;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Implements the matrix-vector multiplication program that shows how to use GPU resources in Flink.
 *
 * <p>The input is a vector stream from a {@link RandomVectorSource}, which will generate random vectors with specified
 * dimension. The data size of the vector stream could be specified by user. Each vector will be multiplied with a random
 * dimension * dimension matrix in {@link Multiplier} and the result would be emitted to output.
 *
 * <p>Usage: MatrixVectorMul [--output &lt;path&gt;] [--dimension &lt;dimension&gt; --data-size &lt;data_size&gt;]
 *
 * <p>If no parameters are provided, the program is run with default vector dimension 10 and data size 100.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>leverage external resource in operators,
 * <li>accelerate complex calculation with GPU resources.
 * </ul>
 *
 * <p>Notice that you need to add JCuda natives libraries in your Flink distribution by the following steps:
 * <ul>
 * <li>download the JCuda native libraries bundle for your CUDA version from http://www.jcuda.org/downloads/
 * <li>copy the native libraries jcuda-natives and jcublas-natives for your CUDA version, operating system and architecture
 * to the "lib/" folder of your Flink distribution
 * </ul>
 */
public class MatrixVectorMul {

	private static final int DEFAULT_DIM = 10;
	private static final int DEFAULT_DATA_SIZE = 100;
	private static final String DEFAULT_RESOURCE_NAME = "gpu";

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		System.out.println("Usage: MatrixVectorMul [--output <path>] [--dimension <dimension> --data-size <data_size>] [--resource-name <resource_name>]");

		// Set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		final int dimension = params.getInt("dimension", DEFAULT_DIM);
		final int dataSize = params.getInt("data-size", DEFAULT_DATA_SIZE);
		final String resourceName = params.get("resource-name", DEFAULT_RESOURCE_NAME);

		DataStream<List<Float>> result = env.addSource(new RandomVectorSource(dimension, dataSize))
						.map(new Multiplier(dimension, resourceName));

		// Emit result
		if (params.has("output")) {
			result.addSink(StreamingFileSink.forRowFormat(new Path(params.get("output")),
					new SimpleStringEncoder<List<Float>>()).build());
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			result.print();
		}
		// Execute program
		env.execute("Matrix-Vector Multiplication");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Random vector source which generates random vectors with specified dimension and total data size.
	 */
	private static final class RandomVectorSource extends RichSourceFunction<List<Float>> {

		private transient volatile boolean running;
		private final int dimension;
		private final int dataSize;

		RandomVectorSource(int dimension, int dataSize) {
			this.dimension = dimension;
			this.dataSize = dataSize;
		}

		@Override
		public void open(Configuration parameters) {
			running = true;
		}

		@Override
		public void run(SourceContext<List<Float>> ctx) {
			int count = 0;
			while (running && count < dataSize) {
				List<Float> randomRecord = new ArrayList<>();
				for (int i = 0; i < dimension; ++i) {
					randomRecord.add((float) Math.random());
				}
				ctx.collect(randomRecord);
				count += 1;
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	/**
	 * Matrix-Vector multiplier using CUBLAS library.
	 */
	private static final class Multiplier extends RichMapFunction<List<Float>, List<Float>> {
		private final int dimension;
		private final String resourceName;
		private Pointer matrixPointer;

		Multiplier(int dimension, String resourceName) {
			this.dimension = dimension;
			this.resourceName = resourceName;
		}

		@Override
		public void open(Configuration parameters) {
			// When multiple instances of this class and JCuda exist in different class loaders, then we will get UnsatisfiedLinkError.
			// To avoid that, we need to temporarily override the java.io.tmpdir, where the JCuda store its native library, with a random path.
			// For more details please refer to https://issues.apache.org/jira/browse/FLINK-5408 and the discussion in http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/Classloader-and-removal-of-native-libraries-td14808.html
			final String originTempDir = System.getProperty("java.io.tmpdir");
			final String newTempDir = originTempDir + "/jcuda-" + UUID.randomUUID();
			System.setProperty("java.io.tmpdir", newTempDir);

			final Set<ExternalResourceInfo> externalResourceInfos = getRuntimeContext().getExternalResourceInfos(resourceName);
			Preconditions.checkState(!externalResourceInfos.isEmpty(), "The MatrixVectorMul needs at least one GPU device while finding 0 GPU.");
			final Optional<String> firstIndexOptional = externalResourceInfos.iterator().next().getProperty("index");
			Preconditions.checkState(firstIndexOptional.isPresent());

			matrixPointer = new Pointer();
			final float[] matrix = new float[dimension * dimension];
			// Initialize a random matrix
			for (int i = 0; i < dimension * dimension; ++i) {
				matrix[i] = (float) Math.random();
			}

			// Set the CUDA device
			JCuda.cudaSetDevice(Integer.parseInt(firstIndexOptional.get()));

			// Initialize JCublas
			JCublas.cublasInit();

			// Allocate device memory for the matrix
			JCublas.cublasAlloc(dimension * dimension, Sizeof.FLOAT, matrixPointer);
			JCublas.cublasSetVector(dimension * dimension, Sizeof.FLOAT, Pointer.to(matrix), 1, matrixPointer, 1);

			// Change the java.io.tmpdir back to its original value.
			System.setProperty("java.io.tmpdir", originTempDir);
		}

		@Override
		public List<Float> map(List<Float> value) {
			final float[] input = new float[dimension];
			final float[] output = new float[dimension];
			final Pointer inputPointer = new Pointer();
			final Pointer outputPointer = new Pointer();

			// Fill the input and output vector
			for (int i = 0; i < dimension; i++) {
				input[i] = value.get(i);
				output[i] = 0;
			}

			// Allocate device memory for the input and output
			JCublas.cublasAlloc(dimension, Sizeof.FLOAT, inputPointer);
			JCublas.cublasAlloc(dimension, Sizeof.FLOAT, outputPointer);

			// Initialize the device matrices
			JCublas.cublasSetVector(dimension, Sizeof.FLOAT, Pointer.to(input), 1, inputPointer, 1);
			JCublas.cublasSetVector(dimension, Sizeof.FLOAT, Pointer.to(output), 1, outputPointer, 1);

			// Performs operation using JCublas
			JCublas.cublasSgemv('n', dimension, dimension, 1.0f,
				matrixPointer, dimension, inputPointer, 1, 0.0f, outputPointer, 1);

			// Read the result back
			JCublas.cublasGetVector(dimension, Sizeof.FLOAT, outputPointer, 1, Pointer.to(output), 1);

			// Memory clean up
			JCublas.cublasFree(inputPointer);
			JCublas.cublasFree(outputPointer);

			List<Float> outputList = new ArrayList<>();
			for (int i = 0; i < dimension; ++i) {
				outputList.add(output[i]);
			}

			return outputList;
		}

		@Override
		public void close() {
			// Memory clean up
			JCublas.cublasFree(matrixPointer);

			// Shutdown cublas
			JCublas.cublasShutdown();
		}
	}
}
