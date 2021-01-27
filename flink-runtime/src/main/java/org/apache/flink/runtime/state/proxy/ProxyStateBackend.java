package org.apache.flink.runtime.state.proxy;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.runtime.state.StateBackendLoader.fromApplicationOrConfigOrDefault;

/***/
public class ProxyStateBackend implements StateBackend {
	private static final Logger LOG = LoggerFactory.getLogger(ProxyStateBackend.class);

	final StateBackend stateBackend;

	public ProxyStateBackend(@Nullable StateBackend fromApplication,
					  Configuration config,
					  ClassLoader classLoader,
					  @Nullable Logger logger)
		throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {
		this.stateBackend = fromApplicationOrConfigOrDefault(fromApplication, config, classLoader, logger);
	}

	public ProxyStateBackend(StateBackend stateBackend)
		throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {
		this.stateBackend = stateBackend;
	}

	@Override
	// this is called when StreamTask create KeyedStateBackend
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry,
			TtlTimeProvider ttlTimeProvider,
			MetricGroup metricGroup,
			@Nonnull Collection<KeyedStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry) throws Exception {
		LOG.debug("createKeyedStateBackend is called");
		AbstractKeyedStateBackend<K> keyedStateBackend =
			(AbstractKeyedStateBackend<K>) stateBackend.createKeyedStateBackend(
					env,
					jobID,
					operatorIdentifier,
					keySerializer,
					numberOfKeyGroups,
					keyGroupRange,
					kvStateRegistry,
					ttlTimeProvider,
					metricGroup,
					stateHandles,
					cancelStreamRegistry);
		return new ProxyKeyedStateBackend<>(keyedStateBackend);
	}

	@Override
	public OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier, @Nonnull Collection<OperatorStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) throws Exception {
		LOG.debug("createOperatorStateBackend is called");
		return stateBackend.createOperatorStateBackend(env, operatorIdentifier, stateHandles, cancelStreamRegistry);
	}
}
