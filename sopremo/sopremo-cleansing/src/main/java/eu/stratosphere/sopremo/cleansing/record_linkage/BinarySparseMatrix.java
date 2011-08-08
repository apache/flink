package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonNode;

class BinarySparseMatrix {
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
		return this.sparseMatrix.equals(other.sparseMatrix);
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

	void makeSymmetric() {
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
}