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

import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType1;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType2;

import java.io.File;
import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.input.ExternalProcessFixedLengthInputFormat;
import eu.stratosphere.pact.common.io.input.ExternalProcessInputSplit;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

public abstract class MyriadGeneratorFixedLengthInputFormat<K extends Key,V extends Value> extends ExternalProcessFixedLengthInputFormat<ExternalProcessInputSplit, K, V> {

	/**
	 * Config parameter key for path to myriad generator binary 
	 */
	public static final String GENERATOR_PATH_PARAMETER_KEY = "pact.input.myriad.generator.path";
	
	/**
	 * Config parameter key for scale factor of generated data
	 */
	public static final String GENERATOR_SCALEFACTOR_PARAMETER_KEY = "pact.input.myriad.generator.scalefactor";
	
	/**
	 * Config parameter key for path to myriad generator configuration directory
	 */
	public static final String GENERATOR_CONFIGDIR_PARAMETER_KEY = "pact.input.myriad.generator.configdir";
	
	/**
	 * Config parameter key for the base cardinality per scale factor
	 */
	public static final String GENERATOR_SCALEFACTORBASECARD_PARAMETER_KEY = "pact.input.myriad.generator.scalefactorbasecard";
	
	public static final String PAYLOAD_SIZE_PARAMETER_KEY = "gen.input.format.payloadSize";
	
	/**
	 * Prefix of Myriad's help message. Used to verify the binary.
	 */
	public static final String MYRIAD_HELP_MESSAGE_PREFIX = "usage: skew-node OPTIONS";
	
	protected Class<K> keyClass;
	protected Class<V> valClass;
	
	private String pathToGeneratorBinary;
	private String pathToConfigDir;
	private float scaleFactor;
	private long scaleFactorBaseCard;

	public MyriadGeneratorFixedLengthInputFormat() {
		
		this.keyClass = getTemplateType1(getClass());
		this.valClass = getTemplateType2(getClass());
	}
	
	@Override
	public void configure(Configuration parameters) {
		
		super.configure(parameters);
		
		// check generator binary
		this.pathToGeneratorBinary = parameters.getString(GENERATOR_PATH_PARAMETER_KEY, null);
		if (this.pathToGeneratorBinary == null) {
			throw new IllegalArgumentException("Configuration does not contain a path to a generator binary.");
		}
		File checkFile = new File(this.pathToGeneratorBinary);
		if(!checkFile.exists()) {
			throw new IllegalArgumentException("Configured generator binary does not exist: "+this.pathToGeneratorBinary);
		}
		
		try {
			Process p = Runtime.getRuntime().exec(this.pathToGeneratorBinary+" --help");
			
			byte[] stdOut = new byte[1024*64];
			p.getInputStream().read(stdOut);
			if(!(new String(stdOut).startsWith(MYRIAD_HELP_MESSAGE_PREFIX))) {
				throw new IllegalArgumentException("Specified binary is not a myriad generator: "+this.pathToGeneratorBinary);
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to execute specified binary: "+this.pathToGeneratorBinary);
		}
		
		// check config directory
		this.pathToConfigDir = parameters.getString(GENERATOR_CONFIGDIR_PARAMETER_KEY, null);
		if(this.pathToConfigDir != null) {
		
			checkFile = new File(this.pathToConfigDir);
			if(!checkFile.exists()) {
				throw new IllegalArgumentException("Configured generator configuration directory does not exist: "+this.pathToConfigDir);
			}
			if(!checkFile.isDirectory()) {
				throw new IllegalArgumentException("Configured generator configuration directory is not a directory: "+this.pathToConfigDir);
			}
		}
				
		// check scale factor
		this.scaleFactor = parameters.getFloat(GENERATOR_SCALEFACTOR_PARAMETER_KEY, -1.0f);
		if(this.scaleFactor <= 0.0) {
			throw new IllegalArgumentException("Specified scale factor must be > 0.0");
		}
		
		this.scaleFactorBaseCard = parameters.getLong(GENERATOR_SCALEFACTORBASECARD_PARAMETER_KEY, -1);
		
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#createInputSplits(int)
	 */
	@Override
	public ExternalProcessInputSplit[] createInputSplits(int minNumSplits)
			throws IOException {
		
		ExternalProcessInputSplit[] inputSplits = new ExternalProcessInputSplit[minNumSplits];
		for(int i=0; i < minNumSplits; i++) {
			String extProcessCommand = this.pathToGeneratorBinary+
			" -s"+this.scaleFactor+
			" -N"+minNumSplits+
			" -i"+(i+1);
			if(this.pathToConfigDir != null) {
				extProcessCommand += " -c"+this.pathToConfigDir; 
			}
			
			inputSplits[i] = new ExternalProcessInputSplit(i+1,extProcessCommand);
		}
		
		return inputSplits;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#getInputSplitType()
	 */
	@Override
	public Class<ExternalProcessInputSplit> getInputSplitType() {
		return ExternalProcessInputSplit.class;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#getStatistics(eu.stratosphere.pact.common.io.statistics.BaseStatistics)
	 */
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		
		final long numberOfRecords = (long)(this.scaleFactorBaseCard * this.scaleFactorBaseCard);
		final float avgRecordWidth = super.recordLength;
		final long totalInputSize = (long)(numberOfRecords * avgRecordWidth);
		
		return new BaseStatistics() {
			
			@Override
			public long getTotalInputSize() {
				return totalInputSize;
			}
			
			@Override
			public long getNumberOfRecords() {
				return numberOfRecords;
			}
			
			@Override
			public float getAverageRecordWidth() {
				return avgRecordWidth;
			}
		};
		
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.input.InputFormat#createPair()
	 */
	@Override
	public KeyValuePair<K, V> createPair()
	{
		try {
			return new KeyValuePair<K, V>(this.keyClass.newInstance(), this.valClass.newInstance());
		}
		catch (InstantiationException e) {
			throw new RuntimeException(e);
		}
		catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	/*
	private class StreamSwallower implements Runnable {

		private InputStream swallowStream;
		private int swallowInterval;
		
		public StreamSwallower(InputStream s, int swallowInterval) {
			this.swallowStream = s;
			this.swallowInterval = swallowInterval;
		}
		
		@Override
		public void run() {
			byte[] swallowBuf = new byte[1024];
			int readCnt = 0;

			while(readCnt != -1) {
				try {
					readCnt = swallowStream.read(swallowBuf);
					
					Thread.sleep(this.swallowInterval);
					
				} catch (IOException e) {
					throw new RuntimeException("IOError when swallowing stream");
				} catch (InterruptedException e) {

				}
			}
		}
	}
	*/
	
}
