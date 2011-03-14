package eu.stratosphere.pact.compiler.util;


import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.compiler.DataStatistics;


/**
 * This class overrides the methods used by the DataStatistics Object to determine statistics and returns pre-defined
 * statistics.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class MockDataStatistics extends DataStatistics
{
	private final Map<String, BasicFileStatistics> statsForFiles;
	
	
	public MockDataStatistics() {
		this.statsForFiles = new HashMap<String, DataStatistics.BasicFileStatistics>();
	}
	
	
	public void setStatsForFile(String filePath, BasicFileStatistics stats) {
		this.statsForFiles.put(filePath, stats);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.DataStatistics#getFileStatistics(java.lang.String, eu.stratosphere.pact.common.io.InputFormat)
	 */
	@Override
	public BasicFileStatistics getFileStatistics(String filePath, InputFormat<?, ?> format) {
		return statsForFiles.get(filePath);
	}
	
}
