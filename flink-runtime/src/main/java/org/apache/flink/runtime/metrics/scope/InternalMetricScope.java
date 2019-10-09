package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.MetricScope;
import org.apache.flink.runtime.metrics.DelimiterProvider;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Default scope implementation. Contains additional methods assembling identifiers based on reporter-specific delimiters.
 */
@Internal
public class InternalMetricScope implements MetricScope {

	private final DelimiterProvider delimiterProvider;
	private final Supplier<Map<String, String>> variablesProvider;

	/**
	 * The map containing all variables and their associated values, lazily computed.
	 */
	protected volatile Map<String, String> variables;

	/**
	 * The metrics scope represented by this group.
	 * For example ["host-7", "taskmanager-2", "window_word_count", "my-mapper" ].
	 */
	private final String[] scopeComponents;

	/**
	 * Array containing the metrics scope represented by this group for each reporter, as a concatenated string, lazily computed.
	 * For example: "host-7.taskmanager-2.window_word_count.my-mapper"
	 */
	private final String[] scopeStrings;

	public InternalMetricScope(DelimiterProvider delimiterProvider, String[] scopeComponents, Supplier<Map<String, String>> variablesProvider) {
		this.delimiterProvider = delimiterProvider;
		this.variablesProvider = variablesProvider;
		this.scopeComponents = scopeComponents;
		this.scopeStrings = new String[delimiterProvider.getNumberReporters()];
	}

	@Override
	public Map<String, String> getAllVariables() {
		if (variables == null) { // avoid synchronization for common case
			synchronized (this) {
				if (variables == null) {
					variables = variablesProvider.get();
				}
			}
		}
		return variables;
	}

	public String[] geScopeComponents() {
		return scopeStrings;
	}

	@Override
	public String getMetricIdentifier(String metricName) {
		return getMetricIdentifier(metricName, s -> s, delimiterProvider.getDelimiter(), -1);
	}

	@Override
	public String getMetricIdentifier(String metricName, CharacterFilter filter) {
		return getMetricIdentifier(metricName, filter, delimiterProvider.getDelimiter(), -1);
	}

	@Internal
	public String getMetricIdentifier(String metricName, CharacterFilter filter, int reporterIndex) {
		return getMetricIdentifier(metricName, filter, delimiterProvider.getDelimiter(reporterIndex), reporterIndex);
	}

	private String getMetricIdentifier(String metricName, CharacterFilter filter, char delimiter, int reporterIndex) {
		return getIdentifierScope(filter, delimiter, reporterIndex) + delimiter + filter.filterCharacters(metricName);
	}

	private String getIdentifierScope(CharacterFilter filter, char delimiter, int reporterIndex) {
		if (scopeStrings.length == 0 || (reporterIndex < 0 || reporterIndex >= scopeStrings.length)) {
			return ScopeFormat.concat(filter, delimiter, scopeComponents);
		} else {
			if (scopeStrings[reporterIndex] == null) {
				scopeStrings[reporterIndex] = ScopeFormat.concat(filter, delimiter, scopeComponents);
			}
			return scopeStrings[reporterIndex];
		}
	}
}
