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

package eu.stratosphere.api.io;

import java.io.IOException;
import java.io.Serializable;

import eu.stratosphere.api.io.statistics.BaseStatistics;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.InputSplit;

/**
 * Describes the base interface that is used describe an input that produces records that are processed
 * by stratosphere.
 * <p>
 * The input format handles the following:
 * <ul>
 *   <li>It describes how the input is split into splits that can be processed in parallel.</li>
 *   <li>It describes how to read records from the input split.</li>
 *   <li>It describes how to gather basic statistics from the input.</li> 
 * </ul>
 * <p>
 * The life cycle of an input format is the following:
 * <ol>
 *   <li>After being instantiated (parameterless), it is configured with a {@link Configuration} object. 
 *       Basic fields are read from the configuration, such as for example a file path, if the format describes
 *       files as input.</li>
 *   <li>Optionally: It is called by the compiler to produce basic statistics about the input.</li>
 *   <li>It is called to create the input splits.</li>
 *   <li>Each parallel input task creates an instance, configures it and opens it for a specific split.</li>
 *   <li>All records are read from the input</li>
 *   <li>The input format is closed</li>
 * </ol>
 * <p>
 * IMPORTANT NOTE: Input formats must be written such that an instance can be opened again after it was closed. That
 * is due to the fact that the input format is used for potentially multiple splits. After a split is done, the
 * format's close function is invoked and, if another split is available, the open function is invoked afterwards for
 * the next split.
 *  
 * @see InputSplit
 * @see BaseStatistics
 * 
 * @param <OT> The type of the produced records.
 * @param <T> The type of input split.
 */
public interface InputFormat<OT, T extends InputSplit> extends Serializable {
	
	/**
	 * Configures this input format. Since input formats are instantiated generically and hence parameterless, 
	 * this method is the place where the input formats set their basic fields based on configuration values.
	 * <p>
	 * This method is always called first on a newly instantiated input format. 
	 *  
	 * @param parameters The configuration with all parameters.
	 */
	void configure(Configuration parameters);
	
	/**
	 * Gets the basic statistics from the input described by this format. If the input format does not know how
	 * to create those statistics, it may return null.
	 * This method optionally gets a cached version of the statistics. The input format may examine them and decide
	 * whether it directly returns them without spending effort to re-gather the statistics.
	 * <p>
	 * When this method is called, the input format it guaranteed to be configured.
	 * 
	 * @param cachedStatistics The statistics that were cached. May be null.
	 * @return The base statistics for the input, or null, if not available.
	 */
	BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException;
	
	/**
	 * Creates the different splits of the input that can be processed in parallel.
	 * <p>
	 * When this method is called, the input format it guaranteed to be configured.
	 * 
	 * @param minNumSplits The minimum desired number of splits. If fewer are created, some parallel
	 *                     instances may remain idle.
	 * @return The splits of this input that can be processed in parallel. 
	 * 
	 * @throws Exception Thrown, when the creation of the splits was erroneous.
	 */
	T[] createInputSplits(int minNumSplits) throws IOException;
	
	/**
	 * Gets the type of the input splits that are processed by this input format.
	 * 
	 * @return The type of the input splits.
	 */
	Class<? extends T> getInputSplitType();
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Opens a parallel instance of the input format to work on a split.
	 * <p>
	 * When this method is called, the input format it guaranteed to be configured.
	 * 
	 * @param split The split to be opened.
	 * @throws IOException Thrown, if the spit could not be opened due to an I/O problem.
	 */
	void open(T split) throws IOException;
	
	/**
	 * Method used to check if the end of the input is reached.
	 * <p>
	 * When this method is called, the input format it guaranteed to be opened.
	 * 
	 * @return True if the end is reached, otherwise false.
	 * @throws IOException Thrown, if an I/O error occurred.
	 */
	boolean reachedEnd() throws IOException;
	
	/**
	 * Tries to read the next pair from the input. By using the return value invalid records in the
	 * input can be skipped.
	 * <p>
	 * When this method is called, the input format it guaranteed to be opened.
	 * 
	 * @param record Record into which the next key / value pair will be stored.
	 * @return Indicates whether the record could be successfully read. A return value of <i>true</i>
	 *         indicates that the read was successful, a return value of false indicates that the
	 *         current record was not read successfully and should be skipped.
	 *         
	 * @throws IOException Thrown, if an I/O error occurred.
	 */
	boolean nextRecord(OT record) throws IOException;
	
	/**
	 * Method that marks the end of the life-cycle of an input split. Should be used to close channels and streams
	 * and release resources. After this method returns without an error, the input is assumed to be correctly read.
	 * <p>
	 * When this method is called, the input format it guaranteed to be opened.
	 * 
	 * @throws IOException Thrown, if the input could not be closed properly.
	 */
	void close() throws IOException;
}
