package eu.stratosphere.nephele.streaming.profiling;

import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.DataOutputBuffer;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

public class XoredVertexID extends AbstractID {

	public XoredVertexID(ExecutionVertexID one, ExecutionVertexID two) {
		super(xorAbstractIDs(one, two));
	}

	public XoredVertexID(ManagementVertexID one, ManagementVertexID two) {
		super(xorAbstractIDs(one, two));
	}

	private static byte[] xorAbstractIDs(AbstractID one, AbstractID two) {
		DataOutputBuffer buffer = new DataOutputBuffer(16);
		try {
			one.write(buffer);

			byte[] data = new byte[16];
			System.arraycopy(buffer.getData().array(), 0, data, 0, 16);
			buffer.reset();
			two.write(buffer);

			byte[] twoData = buffer.getData().array();
			for (int i = 0; i < 16; i++) {
				data[i] = (byte) (data[i] ^ twoData[i]);
			}
			return data;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}
