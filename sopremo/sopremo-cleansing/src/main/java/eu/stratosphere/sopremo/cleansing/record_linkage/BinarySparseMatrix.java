package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;

public class BinarySparseMatrix extends JsonNode implements Value {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5533221391825038587L;
	private final Map<JsonNode, Set<JsonNode>> sparseMatrix = new HashMap<JsonNode, Set<JsonNode>>();

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final BinarySparseMatrix other = (BinarySparseMatrix) obj;
		if (this.sparseMatrix.size() != other.sparseMatrix.size())
			return false;

		Set<JsonNode> rows = this.getRows();
		for (JsonNode row : rows)
			if (!this.get(row).equals(this.sparseMatrix.get(row)))
				return false;
		return true;
	}

	public Set<JsonNode> get(final JsonNode n) {
		return this.sparseMatrix.get(n);
	}

	public Set<JsonNode> getRows() {
		return this.sparseMatrix.keySet();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.sparseMatrix.hashCode();
		return result;
	}

	public boolean isSet(final JsonNode n1, final JsonNode n2) {
		final Set<JsonNode> set = this.sparseMatrix.get(n1);
		return set != null && set.contains(n2);
	}

	public void makeSymmetric() {
		final Set<JsonNode> rows = new HashSet<JsonNode>(this.getRows());
		for (final JsonNode row : rows)
			for (final JsonNode column : this.get(row))
				this.set(column, row);
	}

	public void set(final JsonNode n1, final JsonNode n2) {
		Set<JsonNode> set = this.sparseMatrix.get(n1);
		if (set == null)
			this.sparseMatrix.put(n1, set = new HashSet<JsonNode>());
		set.add(n2);
	}

	public void setAll(final JsonNode n1, final Set<JsonNode> n2) {
		Set<JsonNode> set = this.sparseMatrix.get(n1);
		if (set == null)
			this.sparseMatrix.put(n1, set = new HashSet<JsonNode>());
		set.addAll(n2);
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder("[\n");
		for (final JsonNode row : this.getRows())
			builder.append("[").append(row).append(": ").append(this.get(row)).append("]\n");
		return builder.append("]").toString();
	}

	public void clear() {
		this.sparseMatrix.clear();
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.sparseMatrix.clear();
		final int len = in.readInt();

		JsonNodeWrapper wrapper = new JsonNodeWrapper();
		JsonNodeWrapper innerWrapper = new JsonNodeWrapper();
		JsonNode node;
		for (int i = 0; i < len; i++) {

			wrapper.read(in);
			node = wrapper.getValue();

			int rowLen = in.readInt();
			for (int j = 0; j < rowLen; j++) {
				innerWrapper.read(in);
				this.set(node, innerWrapper.getValue());

			}

		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.sparseMatrix.size());

		JsonNodeWrapper wrapper = new JsonNodeWrapper();
		// iterate over all rows
		for (final Entry<JsonNode, Set<JsonNode>> entry : this.sparseMatrix.entrySet()) {
			// write out row key
			wrapper.setValue(entry.getKey());
			wrapper.write(out);

			// iterate over all columns
			Set<JsonNode> rowEntries = entry.getValue();
			out.writeInt(rowEntries.size());
			for (JsonNode rowEntry : rowEntries) {
				wrapper.setValue(rowEntry);
				wrapper.write(out);
			}
		}
	}
	
	@Override
	public int getTypePos() {
		return TYPES.CustomNode.ordinal();
	}

	@Override
	public TYPES getType() {
		return TYPES.CustomNode;
	}

	@Override
	public int compareToSameType(JsonNode other) {
		throw new UnsupportedOperationException("BinarySparseMatrix isn't comparable.");
	}

	@Override
	public StringBuilder toString(StringBuilder sb) {
		return null;
	}
}