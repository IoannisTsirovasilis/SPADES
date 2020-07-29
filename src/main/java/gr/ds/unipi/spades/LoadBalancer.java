package gr.ds.unipi.spades;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.spark.Partitioner;
import org.apache.spark.broadcast.Broadcast;

public class LoadBalancer extends Partitioner {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2828039002390466153L;
	private int numPartitions;
	public HashMap<Integer, Integer> loads; 
	public HashMap<Integer, Integer> loadsSizes;
	
	public LoadBalancer(int numPartitions) {
		super();
		this.numPartitions = numPartitions;
	}
	
	public LoadBalancer(int numPartitions, Broadcast<SpatioTextualJoin> broadcastStj) {
		super();
		this.numPartitions = numPartitions;
		SpatioTextualJoin stj = broadcastStj.getValue();
		assignDataToReducer(stj.getBins());
	}
	
	public LoadBalancer(int numPartitions, HashMap<Integer, Integer> bins) {
		super();
		this.numPartitions = numPartitions;
		assignDataToReducer(bins);
	}
	
	public void assignDataToReducer(HashMap<Integer, Integer> bins) 
	{
		loads = new HashMap<Integer, Integer>();
		loadsSizes = new HashMap<Integer, Integer>();
		bins = sortBins(bins);
		
	    int i = 0 ;
	    
	    for (Integer key : bins.keySet()) {
	    	if (i < numPartitions) {
	    		loads.put(key, i);
	    		loadsSizes.put(i, bins.get(key));
	    		i++;
	    	} else {
	    		Integer minKey = minLoadsSizes();
	    		loads.put(key, minKey);
	    		loadsSizes.replace(minKey, new Integer(loadsSizes.get(minKey).intValue() + bins.get(key).intValue()));
	    	}
	    }
	}

	@Override
	public int getPartition(Object key) {
		if (key.getClass() != Integer.class) throw new IllegalArgumentException("Wrong key type provided");
		if (!loads.containsKey((Integer) key)) return 0;
		return loads.get((Integer)key).intValue();
	}

	@Override
	public int numPartitions() {
		return numPartitions;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof LoadBalancer) {
			return ((LoadBalancer) obj).numPartitions == numPartitions;
		}
		return false;
	}
	
	private HashMap<Integer, Integer> sortBins(Map<Integer, Integer> bins)
	{
	    List<Integer> mapKeys = new ArrayList<Integer>(bins.keySet());
	    List<Integer> mapValues = new ArrayList<Integer>(bins.values());
	    HashMap<Integer, Integer> sortedMap = new LinkedHashMap<Integer, Integer>();
	    TreeSet<Integer> sortedSet = new TreeSet<Integer>(mapValues);
	    Object[] sortedArray = sortedSet.toArray();
	    int size = sortedArray.length;
	    for (int i= size - 1; i >= 0; i--){
	        sortedMap.put(mapKeys.get(mapValues.indexOf(sortedArray[i])), (Integer)sortedArray[i]);
	    }
	    return sortedMap;
	}
	
	private Integer minLoadsSizes(){
		int min = Integer.MAX_VALUE;
		Integer minKey = null;
		for (Integer key : loadsSizes.keySet()) {
			if (loadsSizes.get(key).compareTo(min) < 0) {
				min = loadsSizes.get(key);
				minKey = key;
			}
		}
		return minKey;
	}
	
	public static void main(String[] args) {
		HashMap<Integer, Integer> bins = new HashMap<Integer, Integer>();
		bins.put(-1, 1000);
		bins.put(0, 2000);
		bins.put(3, 1900);
		bins.put(2, 100);
		bins.put(9, 5500);
		bins.put(8, 30);
		bins.put(1, 890);
		bins.put(4, 988);
		bins.put(7, 4000);
		LoadBalancer lb = new LoadBalancer(4, bins);
		for (Integer p : lb.loads.keySet()) {
			System.out.println("Id: " + p.intValue() + " is assigned to: " + lb.loads.get(p));
		}
		
		for (Integer p : lb.loadsSizes.keySet()) {
			System.out.println("Partition with id: " + p.intValue() + " contains " + lb.loadsSizes.get(p) + " cells");
		}
	}

}
