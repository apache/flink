package eu.stratosphere.pact.common.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.FormatUtil;

/**
 * Workaround for the current limitation of the {@link Plan} allowing only exactly one {@link DataSinkContract}.
 * This output format simulates a data sink and forwards the data to be written to the configured sub-
 * {@link OutputFormat}s.<br>
 * <br>
 * The SplittingOutputFormat must be used with a valid {@link Configuration} initialized with
 * {@link #addOutputFormat(Configuration, Class, String, Configuration)}.
 * 
 * @author Arvid Heise
 */
public class SplittingOutputFormat extends OutputFormat<Key, Value> {
	private final List<OutputFormat<Key, Value>> formats = new ArrayList<OutputFormat<Key, Value>>();

	@Override
	public void close() throws IOException {
		for (final OutputFormat<Key, Value> format : this.formats)
			format.close();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void configure(final Configuration parameters) {
		int count = parameters.getInteger("#outputFormats", 0);
		for (int index = 1; index <= count; index++) {
			final Class<? extends OutputFormat> outputFormatClass = (Class<? extends OutputFormat>) parameters
				.getClass("outputFormat" + index, null);
			String path = parameters.getString("outputPath" + index, null);
			final String subconfigurationString = parameters.getString("outputConfiguration" + index, null);
			Configuration subconfiguration = subconfigurationString == null ? null : FormatUtil.fromString(
				Configuration.class, subconfigurationString);
			try {
				this.formats.add(FormatUtil.createOutputFormat(outputFormatClass, path, subconfiguration));
			} catch (final IOException e) {
				throw new IllegalStateException("Cannot open configured output path " + path);
			}
		}
	}

	@Override
	protected void initTypes() {
		super.ok = Key.class;
		super.ov = Value.class;
	}

	@Override
	public void open() throws IOException {
		for (final OutputFormat<Key, Value> format : this.formats)
			format.open();
	}

	@Override
	public void writePair(final KeyValuePair<Key, Value> pair) throws IOException {
		for (final OutputFormat<Key, Value> format : this.formats)
			format.writePair(pair);
	}

	/**
	 * Adds a data sink with the given {@link OutputFormat}, path, and {@link Configuration} to the configuration of a
	 * SplittingOutputFormat.
	 * 
	 * @param configuration
	 *        the configuration of the SplittingOutputFormat
	 * @param outputFormat
	 *        the format of the data sink
	 * @param path
	 *        the path of the data sink
	 * @param outputConfiguration
	 *        the configuration of the format, may be null
	 */
	@SuppressWarnings({ "rawtypes" })
	public static void addOutputFormat(final Configuration configuration,
			final Class<? extends OutputFormat> outputFormat, final String path, final Configuration outputConfiguration) {
		int currentIndex = configuration.getInteger("#outputFormats", 0) + 1;
		configuration.setClass("outputFormat" + currentIndex, outputFormat);
		configuration.setString("outputPath" + currentIndex, path);
		if (outputConfiguration != null)
			configuration.setString("outputConfiguration" + currentIndex, FormatUtil
				.toString(outputConfiguration));
		configuration.setInteger("#outputFormats", currentIndex);
	}

}
