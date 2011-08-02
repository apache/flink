package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonNode;

class BinarySparseMatrix {
	private Map<JsonNode, Set<JsonNode>> sparseMatrix = new HashMap<JsonNode, Set<JsonNode>>();

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		BinarySparseMatrix other = (BinarySparseMatrix) obj;
		return this.sparseMatrix.equals(other.sparseMatrix);
	}

	public Set<JsonNode> get(JsonNode n) {
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

	public boolean isSet(JsonNode n1, JsonNode n2) {
		Set<JsonNode> set = this.sparseMatrix.get(n1);
		return set != null && set.contains(n2);
	}

	public void set(JsonNode n1, JsonNode n2) {
		Set<JsonNode> set = this.sparseMatrix.get(n1);
		if (set == null)
			this.sparseMatrix.put(n1, set = new HashSet<JsonNode>());
		set.add(n2);
	}

	public void setAll(JsonNode n1, Set<JsonNode> n2) {
		Set<JsonNode> set = this.sparseMatrix.get(n1);
		if (set == null)
			this.sparseMatrix.put(n1, set = new HashSet<JsonNode>());
		set.addAll(n2);
	}

	void makeSymmetric() {
		Set<JsonNode> rows = new HashSet<JsonNode>(getRows());
		for (JsonNode row : rows) {
			for (JsonNode column : get(row)) {
				set(column, row);
			}
		}
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("[\n");
		for (JsonNode row : getRows()) {
			builder.append("[").append(row).append(": ").append(get(row)).append("]\n");
		}
		return builder.append("]").toString();
	}
}