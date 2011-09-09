package eu.stratosphere.pact.common.type.base;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

public class KeyValuePairTest {

	@Mock
	Key myKey;

	@Mock
	Value myValue;

	private DataInputStream in;

	private DataOutputStream out;

	@Before
	public void setUp() {
		initMocks(this);
		try {
			PipedInputStream input = new PipedInputStream(1000);
			in = new DataInputStream(input);
			out = new DataOutputStream(new PipedOutputStream(input));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testKeyValuePair() throws IOException {
		KeyValuePair<Key, Value> toTest = new KeyValuePair<Key, Value>();
		toTest.setKey(myKey);
		toTest.setValue(myValue);

		when(myKey.toString()).thenReturn("");
		toTest.write(out);
		verify(myKey, times(1)).write(Matchers.any(DataOutput.class));
		verify(myValue, times(1)).write(Matchers.any(DataOutput.class));

		toTest.read(in);
		verify(myKey, times(1)).read(Matchers.any(DataInput.class));
		verify(myValue, times(1)).read(Matchers.any(DataInput.class));
	}

}
