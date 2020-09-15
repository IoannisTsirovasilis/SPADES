package gr.ds.unipi.spades.queries;

import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

public class Query {	
	protected int jaccardCount;
	protected int haversineCount;
	protected int pairsCount;
	protected int quadTreeDuplications;
	protected int regularGridDuplications;
	protected int mbrCount;
	protected HashMap<Integer, Integer> bins = new HashMap<Integer, Integer>();
	
	// File 1 Fields
	protected int file1LonIndex, file1LatIndex;
	
	// File 2 Fields
	protected int file2LonIndex, file2LatIndex; 
	
	// File separator (must be the same for both files)
	protected String separator; 
	
	// Index of tag in each record of a file (must be the same for both files)
	protected int tagIndex;
	
	public Query(int file1LonIndex, int file1LatIndex, int file2LonIndex, 
			int file2LatIndex, String separator, int tagIndex) {
		this.file1LonIndex = file1LonIndex;
		this.file1LatIndex = file1LatIndex;
		this.file2LonIndex = file2LonIndex;
		this.file2LatIndex = file2LatIndex;
		this.separator = separator;
		this.tagIndex = tagIndex;
	}
	
	public HashMap<Integer, Integer> getBins() {
		return bins;
	}
	
	public void resetBins() {
		bins.clear();
	}
	
	public void insertToBins(Integer key, Integer value) {
		if (bins.containsKey(key)) return;
		
		bins.put(key, value);
	}
	
	public void incrementBinsKey(Integer key) {
		if (!bins.containsKey(key)) { 
			insertToBins(key, new Integer(1));
			return;
		}
		bins.replace(key, new Integer(bins.get(key).intValue() + 1));
	}
	
	// Method for extracting a substring of a delimited String 
	protected String extractWord(String record, int index, String separator) {
		String word;
		int ordinalIndex;
		if (index == 0) {
			word = record.substring(0, record.indexOf(separator));
		} else {
			ordinalIndex = StringUtils.ordinalIndexOf(record, separator, index);
			
			if (ordinalIndex == StringUtils.lastOrdinalIndexOf(record, separator, 1))
    		{
				word =  record.substring(ordinalIndex + 1);
    		} else {
    			word = record.substring(ordinalIndex + 1, StringUtils.ordinalIndexOf(record, separator, index + 1));
    		}    			
		}
		
		return word;
	}
		
}
