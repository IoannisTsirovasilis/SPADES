package gr.ds.unipi.spades;

import java.util.HashMap;

import org.apache.spark.Partitioner;

public class RoundRobin extends Partitioner {
	
	private int numPartitions;
	public HashMap<Integer, Integer> loads = new HashMap<Integer, Integer>();
	public HashMap<Integer, Integer> loadsSizes = new HashMap<Integer, Integer>();
	
	public RoundRobin(int numPartitions) {
		super();
		this.numPartitions = numPartitions;
	}
	
	public void assignDataToReducer(HashMap<Integer, Integer> bins) 
	{
		loads.clear();
		loadsSizes.clear();
		
	    int i = 0 ;
	    int partition;
	    for (Integer key : bins.keySet()) {
	    	partition = i++ % numPartitions;
	    	loads.put(key, partition);
	    	if (loadsSizes.containsKey(partition))
	    		loadsSizes.replace(partition, new Integer(loadsSizes.get(partition).intValue() + bins.get(key).intValue()));
	    	else
	    		loadsSizes.put(partition, bins.get(key));
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
		if (obj instanceof RoundRobin) {
			return ((RoundRobin) obj).numPartitions == numPartitions;
		}
		return false;
	}
	
	public Integer min(){		
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
	
	public Integer max(){
		int max = Integer.MIN_VALUE;
		Integer maxKey = null;
		for (Integer key : loadsSizes.keySet()) {
			if (loadsSizes.get(key).compareTo(max) > 0) {
				max = loadsSizes.get(key);
				maxKey = key;
			}
		}
		return maxKey;
	}
	
	public double mean(){
		double mean = 0;
		for (Integer key : loadsSizes.keySet()) {
			mean += loadsSizes.get(key);
		}
		return mean / loadsSizes.size();
	}
	
	public double std(){
		double mean = mean();
		double std = 0;
		for (Integer key : loadsSizes.keySet()) {
			std += Math.pow(loadsSizes.get(key) - mean, 2);
		}
		return Math.sqrt(std / loadsSizes.size());
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
		RoundRobin rr = new RoundRobin(4);
		rr.assignDataToReducer(bins);
		
		for (Integer p : rr.loads.keySet()) {
			System.out.println("Id: " + p.intValue() + " is assigned to: " + rr.loads.get(p));
		}
		
		for (Integer p : rr.loadsSizes.keySet()) {
			System.out.println("Partition with id: " + p.intValue() + " contains " + rr.loadsSizes.get(p) + " cells");
		}
		
	}

}
