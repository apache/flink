package eu.stratosphere.sopremo;

import java.util.Arrays;
import java.util.List;

import nl.jqno.equalsverifier.EqualsVerifier;

import org.junit.Ignore;

@Ignore
public class StreamArrayNodeTest extends SopremoTest<StreamArrayNode> {
	@Override
	protected StreamArrayNode createDefaultInstance(final int index) {
		return createStreamArray(index);
	}

	@Override
	protected void initVerifier(final EqualsVerifier<StreamArrayNode> equalVerifier) {
		super.initVerifier(equalVerifier);
		equalVerifier.withPrefabValues(List.class, Arrays.asList(createPactJsonValue("red")),
			Arrays.asList(createPactJsonValue("black")));
		equalVerifier.withPrefabValues(StreamArrayNode.class, createStreamArray("red"), createStreamArray("black"));
	}
}
