package eu.stratosphere.compiler.deadlockdetect;

import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.compiler.plan.PlanNode;

public class DeadlockVertex {
	
	private PlanNode original;
	   
	private List<DeadlockEdge> outEdges;
	   
	private int inDegree;
	   
	public DeadlockVertex( PlanNode original ) {
		this.original = original;
		outEdges = new LinkedList<DeadlockEdge>();
		inDegree = 0;
	}
	   
	public void addEdge(DeadlockVertex destination) {
		
		// no duplicates
		for(DeadlockEdge e : outEdges) {
			if(e.getDestination().equals(destination))
				return;
		}
		
		DeadlockEdge e = new DeadlockEdge(destination);
		this.outEdges.add(e);
	}
	   
	public boolean equals(Object o) {
		   
		if(!(o instanceof DeadlockVertex))
			return false;
		   
		DeadlockVertex v = (DeadlockVertex) o;
		if(v.getOriginal().equals(this.getOriginal()))
			return true;
		
		return false;
	}
	
	public int hashCode() {
		return this.original.hashCode();
	}
	
	public String toString() {
		return original.toString();
	}
	public PlanNode getOriginal() {
		return original;
	}

	public void setOriginal(PlanNode original) {
		this.original = original;
	}

	public int getInDegree() {
		return inDegree;
	}

	public void setInDegree(int inDegree) {
		this.inDegree = inDegree;
	}
	
	public List<DeadlockEdge> getOutEdges() {
		return outEdges;
	}

	public void setOutEdges(List<DeadlockEdge> outEdges) {
		this.outEdges = outEdges;
	}
}
