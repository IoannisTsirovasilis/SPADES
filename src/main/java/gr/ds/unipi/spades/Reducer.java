package gr.ds.unipi.spades;

import java.io.Serializable;
import java.util.List;

public class Reducer implements Comparable<Reducer>, Serializable{
	private Integer reducerToSend;
	private List<Integer> relation;
	private int size;
	
	public Reducer() {
		super();
	}

	public Reducer(List<Integer> relation, int size) {
		super();
		this.relation = relation;
		this.size = size;
	}
	
	public Reducer(Integer reducerToSend, List<Integer> relation, int size) {
		super();
		this.reducerToSend = reducerToSend;
		this.relation = relation;
		this.size = size;
	}

	public List<Integer> getRelation() {
		return relation;
	}

	public void setRelation(List<Integer> relation) {
		this.relation = relation;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	@Override
	public int compareTo(Reducer o) {
		return (this.size < o.size) ? 1 : 0;
	}

	public Integer getReducerToSend() {
		return reducerToSend;
	}

	public void setReducerToSend(Integer reducerToSend) {
		this.reducerToSend = reducerToSend;
	}
	

}
