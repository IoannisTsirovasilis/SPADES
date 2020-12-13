package gr.ds.unipi.spades;

import java.awt.Toolkit;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import gr.ds.unipi.spades.util.IntArrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import gr.ds.unipi.spades.geometry.DataObject;
import gr.ds.unipi.spades.geometry.FeatureObject;
import gr.ds.unipi.spades.geometry.Point;
import gr.ds.unipi.spades.quadTree.Node;
import gr.ds.unipi.spades.quadTree.QuadTree;
import gr.ds.unipi.spades.queries.SpatioTextualJoin;
import gr.ds.unipi.spades.regularGrid.RegularGrid;
import gr.ds.unipi.spades.util.MathUtils;
import scala.Tuple2;

public class LoadBalanceApp {
//	long startTime;
//	long indexCreationTime;
//	long partitions;
//	static int numberOfWorkers = 10	;
//	double duplicates;
//	int inputSize = 250_000;
//	int[] workers;
//	int rgBalanceIndex = 0;
	
	static int numberOfRunsPerFileSet;
	static SpatioTextualJoin stj;
	static JavaSparkContext sc;
	static double minX, minY, maxX, maxY;
	static int[] workers;
	static FileWriter csvWriter;
	static JavaRDD<FeatureObject> points;
	static LoadBalancer lb;
	static RoundRobin rr;
	static String FILE_PATH, file1, file2, localFilePath;
	static int numberOfWorkers, sampleSize, inputSize;
	static double samplePercentage;
	static long startingTime;
	static double toSecondsFactor = Math.pow(10, 9);
	static long partitions, duplicates, outPairs;
	
	private static void regularGridTest(int gridSize) throws IOException {
		int hSectors = gridSize; 
		int vSectors = gridSize;
		long startTime, indexCreationTime, duplicates, partitions;
		double radius = 0;
		int rgBalanceIndex;
		for (int i = 0; i < 3; i++) {
    		try {
    			System.out.println((i + 1) + " iteration...");  
        		if (i == 0) {
        			radius = 2;
        		} else if (i == 1) {
        			radius = 4;
        		} else {
        			radius = 6;
        		}
        		
        		// ------------ REGULAR GRID ---------------
        		for (int c = 0; c < workers.length; c++) workers[c] = 0;
            	System.out.println("Creating regular grid...");
            	startTime = System.nanoTime();
            	
            	RegularGrid grid = stj.createRegularGrid(minX, minY, maxX, maxY, hSectors, vSectors);
            	indexCreationTime = System.nanoTime() - startTime;  
            	System.out.println("Regular grid created...");
            	// Broadcast regular grid
            	Broadcast<RegularGrid> broadcastRegularGrid = sc.broadcast(grid);
            	
            	// Map points to cells
            	JavaPairRDD<Integer, FeatureObject> pairs = stj.map(points, broadcastRegularGrid, radius);               
            	
            	// Group By Key
            	JavaPairRDD<Integer, List<FeatureObject>> groupedPairs = pairs.groupByKey().mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	List<Tuple2<Integer, Integer>> counts = groupedPairs.mapValues(iter -> {
                    List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                    return pp.size();
                }).collect();     
                
                Iterator<Tuple2<Integer, Integer>> iterator = counts.iterator();
            	Tuple2<Integer, Integer> n;
            	Integer load;
            	rgBalanceIndex = 0;
            	while (iterator.hasNext()) {
            		n = iterator.next();
            		load = rgBalanceIndex++ % workers.length;
        			workers[load] +=  n._2;
            	}
            	
            	// Calculate duplicates
            	System.out.println("Counting duplicates (RG)...");
            	duplicates = pairs.count() - inputSize;
            	System.out.println("Duplicates counted (RG)...");
            	
            	System.out.println("Counting partitions (RG)");            	
            	partitions = groupedPairs.keys().count();
            	System.out.println("Partitions counted (RG)...");
            	
            	csvWriter.append("RG" + (int) (gridSize / 10) + "," + inputSize + ",," + radius + "," + hSectors + "x" + vSectors + ",," + 
        				partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
        				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "\n");                   	            	           	
    		} catch (Exception ex) {
    			System.out.println(ex.toString());
    			Toolkit.getDefaultToolkit().beep();
    		} finally {
    			Toolkit.getDefaultToolkit().beep();    			
    		}
    	}
	}
	
	private static void quadTreeTest(int samplePointsPerLeaf, int type) throws IOException {
		double radius = 0;
		//long startTime, indexCreationTime, duplicates, partitions;
		int load;
		FileWriter fw = null;
		for (int i = 0; i < numberOfRunsPerFileSet; i++) {
    		try {
    			System.out.println((i + 1) + " iteration...");
    			fw = new FileWriter(localFilePath + "SpadesFinal/qt_rr_" + type + "_" + i + ".csv");
    			addLabelsWorkers(fw);
    			if (i == 0) {
        			radius = 2;
        		} else if (i == numberOfRunsPerFileSet / 3) {
        			radius = 4;
        		} else if (i == 2 * numberOfRunsPerFileSet / 3) {
        			radius = 6;
        		}
        		
        		// ------------ QUAD TREE (~ CONSTANT NUMBER OF LEAVES) ---------------
            	
            	for (int c = 0; c < workers.length; c++) workers[c] = 0;
            	stj.resetBins();
            	System.out.println("Creating quad tree...");
        		//startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	QuadTree qt = stj.createQuadTreeLPT(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points, radius);
            	//indexCreationTime = System.nanoTime() - startTime;
            	System.out.println("Quad tree created...");
            	rr.assignDataToReducer(stj.getBins());
            	// Broadcast quad tree
            	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
            	// Map points to cells
            	JavaPairRDD<Integer, FeatureObject> pairs = stj.map(points, broadcastQuadTree, radius);            	
            	
            	// Group By Key
            	JavaPairRDD<Integer, List<FeatureObject>> groupedPairs = pairs.groupByKey(rr).mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
        		startingTime = System.nanoTime();
            	
            	outPairs = stj.reduce(groupedPairs, radius).count();
            	
            	startingTime = System.nanoTime() - startingTime;
            	
            	List<Tuple2<Integer, Integer>> counts = groupedPairs.mapValues(iter -> {
                    List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                    return pp.size();
                }).collect();     
                
                Iterator<Tuple2<Integer, Integer>> iterator = counts.iterator();
                Tuple2<Integer, Integer> n;                
            	while (iterator.hasNext()) {
            		n = iterator.next();
            		load = rr.loads.get(n._1);
        			workers[load] +=  n._2;
            	}
            	
            	for (int j = 0; j < workers.length; j++) {
            		fw.append(j + "," + workers[j] + "\n");
            	}
            	
        		System.out.println("Counting duplicates (QT)...");
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	System.out.println("Duplicates counted (QT)...");
            	
            	System.out.println("Counting partitions (QT)...");
            	partitions = groupedPairs.keys().count();
            	System.out.println("Partitions counted (QT)...");
            	
            	
            	
        		csvWriter.append("QT_rr_" + type + "," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
        				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "," + outPairs + "," + (startingTime / toSecondsFactor) + "\n");
            	      		
        		// --------------------- END QUAD TREE MBR CHECK ---------------------        
    		} catch (Exception ex) {
    			Toolkit.getDefaultToolkit().beep();
    		} finally {
    			Toolkit.getDefaultToolkit().beep();    
    			fw.flush();
    			fw.close();
    		}
    	}
	}
	
	private static void quadTreeJSTest(int samplePointsPerLeaf) throws IOException {
		double radius = 0;
		long startTime, indexCreationTime, duplicates, partitions;
		int load;
		FileWriter fw = null;
		for (int i = 0; i < numberOfRunsPerFileSet; i++) {
    		try {
    			System.out.println((i + 1) + " iteration...");
    			fw = new FileWriter(localFilePath + "SJ/qt" + samplePointsPerLeaf + "_" + i + ".csv");
    			addLabelsJS(fw);
        		if (i == 0) {
        			radius = 2;
        		} else if (i == numberOfRunsPerFileSet / 3) {
        			radius = 4;
        		} else if (i == 2 * numberOfRunsPerFileSet / 3) {
        			radius = 6;
        		}
        		
        		// ------------ QUAD TREE (~ CONSTANT NUMBER OF LEAVES) ---------------
            	
            	for (int c = 0; c < workers.length; c++) workers[c] = 0;
            	stj.resetBins();
            	System.out.println("Creating quad tree...");
        		startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	QuadTree qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
            	indexCreationTime = System.nanoTime() - startTime;
            	System.out.println("Quad tree created...");
            	
            	List<Tuple2<Integer, Double>> sampleJSPairs = calculateSampleJoinSelectivity(qt.getRoot(), radius);

            	
            	rr.assignDataToReducer(stj.getBins());
            	// Broadcast quad tree
            	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
            	// Map points to cells
            	JavaPairRDD<Integer, FeatureObject> pairs = stj.map(points, broadcastQuadTree, radius);            	
            	
            	// Group By Key
            	JavaPairRDD<Integer, List<FeatureObject>> groupedPairs = pairs.groupByKey(rr).mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
        		
            	List<Tuple2<Integer, Double>> JSpairs = stj.reduceJoinSelectivity(groupedPairs, radius).collect();
            	
            	for (Tuple2<Integer, Double> samplePair : sampleJSPairs) {
            		for (Tuple2<Integer, Double> pair : JSpairs) {
            			if (pair._1.intValue() == samplePair._1.intValue()) {
            				fw.append(pair._1.intValue() + "," + pair._2 + "," + samplePair._2 + "," + (pair._2 - samplePair._2) + "\n");
            				break;
            			}
            		}
            	}
            	
            	
            	
            	
        		// --------------------- END QUAD TREE MBR CHECK ---------------------   
    		} catch (Throwable e) {
    			e.printStackTrace();
    			Toolkit.getDefaultToolkit().beep();
    		} finally {
    			Toolkit.getDefaultToolkit().beep();    	
    			fw.flush();
            	fw.close();
    		}
    	}
	}
	
	private static void quadTreeDuplicateTest(int samplePointsPerLeaf, int type) throws IOException {
		double radius = 0;
		//long startTime, indexCreationTime, duplicates, partitions;
		int load;
		FileWriter fw = null;
		for (int i = 0; i < numberOfRunsPerFileSet; i++) {
    		try {
    			System.out.println((i + 1) + " iteration...");
    			fw = new FileWriter(localFilePath + "SpadesFinal/qt" + type + "_" + i + ".csv");
    			addLabelsWorkers(fw);
    			if (i == 0) {
        			radius = 2;
        		} else if (i == numberOfRunsPerFileSet / 3) {
        			radius = 4;
        		} else if (i == 2 * numberOfRunsPerFileSet / 3) {
        			radius = 6;
        		}
        		
        		// ------------ QUAD TREE (~ CONSTANT NUMBER OF LEAVES) ---------------
            	
            	for (int c = 0; c < workers.length; c++) workers[c] = 0;
            	stj.resetBins();
            	System.out.println("Creating quad tree...");
        		//startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	QuadTree qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
            	//indexCreationTime = System.nanoTime() - startTime;
            	System.out.println("Quad tree created...");
            	rr.assignDataToReducer(stj.getBins());
            	// Broadcast quad tree
            	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
            	// Map points to cells
            	JavaPairRDD<Integer, FeatureObject> pairs = stj.map(points, broadcastQuadTree, radius);            	
            	
            	// Group By Key
            	JavaPairRDD<Integer, List<FeatureObject>> groupedPairs = pairs.groupByKey(rr).mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
        		startingTime = System.nanoTime();
            	
            	outPairs = stj.reduce(groupedPairs, radius).count();
            	
            	startingTime = System.nanoTime() - startingTime;
            	
            	List<Tuple2<Integer, Integer>> counts = groupedPairs.mapValues(iter -> {
                    List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                    return pp.size();
                }).collect();     
                
                Iterator<Tuple2<Integer, Integer>> iterator = counts.iterator();
                Tuple2<Integer, Integer> n;                
            	while (iterator.hasNext()) {
            		n = iterator.next();
            		load = rr.loads.get(n._1);
        			workers[load] +=  n._2;
            	}
            	
            	for (int j = 0; j < workers.length; j++) {
            		fw.append(j + "," + workers[j] + "\n");
            	}
            	
        		System.out.println("Counting duplicates (QT)...");
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	System.out.println("Duplicates counted (QT)...");
            	
            	System.out.println("Counting partitions (QT)...");
            	partitions = groupedPairs.keys().count();
            	System.out.println("Partitions counted (QT)...");
            	
            	
            	
        		csvWriter.append("QT" + type + "," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
        				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "," + outPairs + "," + (startingTime / toSecondsFactor) + "\n");
            	      		
        		// --------------------- END QUAD TREE MBR CHECK ---------------------        
    		} catch (Exception ex) {
    			Toolkit.getDefaultToolkit().beep();
    		} finally {
    			Toolkit.getDefaultToolkit().beep();    
    			fw.flush();
    			fw.close();
    		}
    	}
	}
	
	private static void lptJSTest(int samplePointsPerLeaf, int type) throws IOException {
		double radius = 0;
		long startTime, indexCreationTime, duplicates, partitions;
		int load;
		FileWriter fw = null;
		for (int i = 0; i < numberOfRunsPerFileSet; i++) {
    		try {
    			System.out.println((i + 1) + " iteration...");
    			fw = new FileWriter(localFilePath + "SJ/lpt" + type + "_" + i + ".csv");
    			addLabelsJS(fw);
        		if (i == 0) {
        			radius = 2;
        		} else if (i == numberOfRunsPerFileSet / 3) {
        			radius = 4;
        		} else if (i == 2 * numberOfRunsPerFileSet / 3) {
        			radius = 6;
        		}
        		
        		// ------------ QUAD TREE (~ CONSTANT NUMBER OF LEAVES) ---------------
            	
            	for (int c = 0; c < workers.length; c++) workers[c] = 0;
            	stj.resetBins();
            	System.out.println("Creating quad tree (LPT)...");
            	startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	QuadTree qt = stj.createQuadTreeLPT(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points, radius);
            	System.out.println("Quad tree created (LPT)...");
            	indexCreationTime = System.nanoTime() - startTime;
            	
            	List<Tuple2<Integer, Double>> sampleJSPairs = calculateSampleJoinSelectivity(qt.getRoot(), radius);
            	
            	lb.assignDataToReducer(stj.getBins());
            	// Broadcast quad tree
            	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
            	
            	// Map points to cells
            	JavaPairRDD<Integer, FeatureObject> pairs = stj.map(points, broadcastQuadTree, radius);     
            	
            	// Group By Key
            	JavaPairRDD<Integer, List<FeatureObject>> groupedPairs = pairs.groupByKey(lb).mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	List<Tuple2<Integer, Integer>> counts = groupedPairs.mapValues(iter -> {
                    List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                    return pp.size();
                }).collect();
        		
            	List<Tuple2<Integer, Double>> JSpairs = stj.reduceJoinSelectivity(groupedPairs, radius).collect();
            	
            	for (Tuple2<Integer, Double> samplePair : sampleJSPairs) {
            		for (Tuple2<Integer, Double> pair : JSpairs) {
            			if (pair._1.intValue() == samplePair._1.intValue()) {
            				fw.append(pair._1.intValue() + "," + pair._2 + "," + samplePair._2 + "," + (pair._2 - samplePair._2) + "\n");
            				break;
            			}
            		}
            	}
            	
        		// --------------------- END QUAD TREE MBR CHECK ---------------------   
    		} catch (Throwable e) {
    			e.printStackTrace();
    			Toolkit.getDefaultToolkit().beep();
    		} finally {
    			Toolkit.getDefaultToolkit().beep();    	
    			fw.flush();
            	fw.close();
    		}
    	}
	}
	
	private static void lptTest(int samplePointsPerLeaf, int type) throws IOException {
		double radius = 0;
		//long partitions, duplicates, startTime, indexCreationTime;
		int load;
		FileWriter fw = null;
		for (int i = 0; i < numberOfRunsPerFileSet; i++) {
    		try {
    			System.out.println((i + 1) + " iteration...");
    			fw = new FileWriter(localFilePath + "SpadesFinal/LPT_n2_" + type + "_" + i + ".csv");
    			addLabelsWorkers(fw);
    			if (i == 0) {
        			radius = 2;
        		} else if (i == numberOfRunsPerFileSet / 3) {
        			radius = 4;
        		} else if (i == 2 * numberOfRunsPerFileSet / 3) {
        			radius = 6;
        		}	
            	// --------------------- QUAD TREE LPT APPROXIMATION ---------------------
            	
            	for (int c = 0; c < workers.length; c++) workers[c] = 0;
            	stj.resetBins();
            	System.out.println("Creating quad tree (LPT)...");
            	//startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	QuadTree qt = stj.createQuadTreeLPT(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points, radius);
            	System.out.println("Quad tree created (LPT)...");
            	//indexCreationTime = System.nanoTime() - startTime;
            	lb.assignDataToReducer(stj.getBins());
            	// Broadcast quad tree
            	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
            	
            	// Map points to cells
            	JavaPairRDD<Integer, FeatureObject> pairs = stj.map(points, broadcastQuadTree, radius);            	
            	
            	// Group By Key
            	JavaPairRDD<Integer, List<FeatureObject>> groupedPairs = pairs.groupByKey(lb).mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	startingTime = System.nanoTime();
            	
            	outPairs = stj.reduce(groupedPairs, radius).count();
            	
            	startingTime = System.nanoTime() - startingTime;
            	
            	List<Tuple2<Integer, Integer>> counts = groupedPairs.mapValues(iter -> {
                    List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                    return pp.size();
                }).collect();     
                
                Iterator<Tuple2<Integer, Integer>> iterator = counts.iterator();
                Tuple2<Integer, Integer> n;
            	while (iterator.hasNext()) {
            		n = iterator.next();
            		load = lb.loads.get(n._1);
        			workers[load] +=  n._2;
            	}
            	
            	for (int j = 0; j < workers.length; j++) {
            		fw.append(j + "," + workers[j] + "\n");
            	}            	

        		System.out.println("Counting duplicates (LPT)...");
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	System.out.println("Duplicates counted (LPT)...");
            	
            	System.out.println("Counting partitions (LPT)...");
            	partitions = groupedPairs.keys().count();
            	System.out.println("Partitions counted (LPT)..."); 
            	
        		csvWriter.append("LPT_n2_" + type + "," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
        				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "," + outPairs + "," + (startingTime / toSecondsFactor) + "\n");
            	
            	
            	// --------------------- END QUAD TREE NORMAL ---------------------
    		} catch (Exception ex) {
    			Toolkit.getDefaultToolkit().beep();
    		} finally {
    			Toolkit.getDefaultToolkit().beep(); 
    			fw.flush();
    			fw.close();
    		}
    	}
	}

	private static void lptDuplicateTest(int samplePointsPerLeaf, int type) throws IOException {
		double radius = 0;
		//long partitions, duplicates, startTime, indexCreationTime;
		int load;
		FileWriter fw = null;
		for (int i = 0; i < numberOfRunsPerFileSet; i++) {
    		try {
    			System.out.println((i + 1) + " iteration...");
    			fw = new FileWriter(localFilePath + "SpadesFinal/lpt_" + type + "_" + i + ".csv");
    			addLabelsWorkers(fw);
    			if (i == 0) {
        			radius = 2;
        		} else if (i == numberOfRunsPerFileSet / 3) {
        			radius = 4;
        		} else if (i == 2 * numberOfRunsPerFileSet / 3) {
        			radius = 6;
        		}	
            	// --------------------- QUAD TREE LPT APPROXIMATION ---------------------
            	
            	for (int c = 0; c < workers.length; c++) workers[c] = 0;
            	stj.resetBins();
            	System.out.println("Creating quad tree (LPT)...");
            	//startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	QuadTree qt = stj.createQuadTreeLPT(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points, radius);
            	System.out.println("Quad tree created (LPT)...");
            	//indexCreationTime = System.nanoTime() - startTime;
            	lb.assignDataToReducer(stj.getBins());
            	// Broadcast quad tree
            	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
            	
            	// Map points to cells
            	JavaPairRDD<Integer, FeatureObject> pairs = stj.map(points, broadcastQuadTree, radius);            	
            	
            	// Group By Key
            	JavaPairRDD<Integer, List<FeatureObject>> groupedPairs = pairs.groupByKey(lb).mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	startingTime = System.nanoTime();
            	
            	outPairs = stj.reduce(groupedPairs, radius).count();
            	
            	startingTime = System.nanoTime() - startingTime;
            	
            	List<Tuple2<Integer, Integer>> counts = groupedPairs.mapValues(iter -> {
                    List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                    return pp.size();
                }).collect();     
                
                Iterator<Tuple2<Integer, Integer>> iterator = counts.iterator();
                Tuple2<Integer, Integer> n;
            	while (iterator.hasNext()) {
            		n = iterator.next();
            		load = lb.loads.get(n._1);
        			workers[load] +=  n._2;
            	}
            	
            	for (int j = 0; j < workers.length; j++) {
            		fw.append(j + "," + workers[j] + "\n");
            	}
            	
        		System.out.println("Counting duplicates (LPT)...");
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	System.out.println("Duplicates counted (LPT)...");
            	
            	System.out.println("Counting partitions (LPT)...");
            	partitions = groupedPairs.keys().count();
            	System.out.println("Partitions counted (LPT)...");            	
            	
        		csvWriter.append("LPT" + type + "," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
        				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "," + outPairs + "," + (startingTime / toSecondsFactor) + "\n");
            	
            	// --------------------- END QUAD TREE NORMAL ---------------------
    		} catch (Exception ex) {
    			Toolkit.getDefaultToolkit().beep();
    		} finally {
    			Toolkit.getDefaultToolkit().beep();    			
    			fw.flush();
    			fw.close();
    		}
    	}
	}
	
	private static void lptNoRadiusTest(int samplePointsPerLeaf, int type) throws IOException {
		double radius = 0;
		//long partitions, duplicates, startTime, indexCreationTime;
		int load;
		FileWriter fw = null;
		for (int i = 0; i < numberOfRunsPerFileSet; i++) {
    		try {
    			System.out.println((i + 1) + " iteration...");
    			fw = new FileWriter(localFilePath + "SpadesFinal/LPT_n1_" + type + "_" + i + ".csv");
    			addLabelsWorkers(fw);
    			if (i == 0) {
        			radius = 2;
        		} else if (i == numberOfRunsPerFileSet / 3) {
        			radius = 4;
        		} else if (i == 2 * numberOfRunsPerFileSet / 3) {
        			radius = 6;
        		}	
            	// --------------------- QUAD TREE LPT APPROXIMATION ---------------------
            	
            	for (int c = 0; c < workers.length; c++) workers[c] = 0;
            	stj.resetBins();
            	System.out.println("Creating quad tree (LPT)...");
            	//startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	QuadTree qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
            	System.out.println("Quad tree created (LPT)...");
            	//indexCreationTime = System.nanoTime() - startTime;
            	lb.assignDataToReducer(stj.getBins());
            	// Broadcast quad tree
            	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
            	
            	// Map points to cells
            	JavaPairRDD<Integer, FeatureObject> pairs = stj.map(points, broadcastQuadTree, radius);            	
            	
            	// Group By Key
            	JavaPairRDD<Integer, List<FeatureObject>> groupedPairs = pairs.groupByKey(lb).mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	List<Tuple2<Integer, Integer>> counts = groupedPairs.mapValues(iter -> {
                    List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                    return pp.size();
                }).collect();     
            	
            	startingTime = System.nanoTime();
            	
            	outPairs = stj.reduce(groupedPairs, radius).count();
            	
            	startingTime = System.nanoTime() - startingTime;
                
                Iterator<Tuple2<Integer, Integer>> iterator = counts.iterator();
                Tuple2<Integer, Integer> n;
            	while (iterator.hasNext()) {
            		n = iterator.next();
            		load = lb.loads.get(n._1);
        			workers[load] +=  n._2;
            	}
            	
            	for (int j = 0; j < workers.length; j++) {
            		fw.append(j + "," + workers[j] + "\n");
            	}

        		System.out.println("Counting duplicates (LPT)...");
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	System.out.println("Duplicates counted (LPT)...");
            	
            	System.out.println("Counting partitions (LPT)...");
            	partitions = groupedPairs.keys().count();
            	System.out.println("Partitions counted (LPT)..."); 
            	
        		csvWriter.append("LPT_nR_" + type + "," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
        				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "," + outPairs + "," + (startingTime / toSecondsFactor) + "\n");
            	
            	
            	// --------------------- END QUAD TREE NORMAL ---------------------
    		} catch (Exception ex) {
    			Toolkit.getDefaultToolkit().beep();
    		} finally {
    			Toolkit.getDefaultToolkit().beep(); 
    			fw.flush();
    			fw.close();
    		}
    	}
	}
	
	private static void geoSparkTest(int samplePointsPerLeaf) {
		int load;
		double radius = 0;
		long partitions, duplicates, indexCreationTime, startTime;
		for (int i = 0; i < numberOfRunsPerFileSet; i++) {
    		try {
    			System.out.println((i + 1) + " iteration...");
    			if (i == 0) {
        			radius = 2;
        		} else if (i == numberOfRunsPerFileSet / 3) {
        			radius = 4;
        		} else if (i == 2 * numberOfRunsPerFileSet / 3) {
        			radius = 6;
        		}
    			
        		// GeoSpark
            	
            	for (int c = 0; c < workers.length; c++) workers[c] = 0;
            	stj.resetBins();
            	System.out.println("Creating quad tree (GS)...");
            	startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	QuadTree qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
            	indexCreationTime = System.nanoTime() - startTime;  
            	System.out.println("Quad tree created (GS)...");
            	rr.assignDataToReducer(stj.getBins());
            	// Broadcast quad tree
            	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
            	
            	// Map points to cells
            	JavaPairRDD<Integer, FeatureObject> pairs = stj.map(points, broadcastQuadTree, radius);            	
            	
            	// Group By Key
            	JavaPairRDD<Integer, List<FeatureObject>> groupedPairs = pairs.groupByKey(rr).mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	List<Tuple2<Integer, Integer>> counts = groupedPairs.mapValues(iter -> {
                    List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                    return pp.size();
                }).collect();     
                
                Iterator<Tuple2<Integer, Integer>> iterator = counts.iterator();
                Tuple2<Integer, Integer> n;
                
            	while (iterator.hasNext()) {
            		n = iterator.next();
            		load = rr.loads.get(n._1);
        			workers[load] +=  n._2;
            	}
            	
        		System.out.println("Counting duplicates (GS)...");
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	System.out.println("Duplicates counted (GS)...");
            	
            	System.out.println("Counting partitions (GS)...");
            	partitions = groupedPairs.keys().count();
            	System.out.println("Partitions counted (GS)...");
        		
            	csvWriter.append("GeoSpark," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
        				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "\n");
    		} catch (Exception e) {
    			Toolkit.getDefaultToolkit().beep();
    		} finally {
    			Toolkit.getDefaultToolkit().beep();
    		}
		}
	}
	
	public static void main(String[] args) throws IOException
    {
    	// Initialize spark context
		numberOfRunsPerFileSet = 18;
		numberOfWorkers = 10;
		inputSize = 250_000;
    	minX = -7.5;
    	minY = -10;
    	maxX = 12.5;
    	maxY = 6;    	
    	samplePercentage = 0.01; 
		file1 = "skewedL_250K.txt";
    	file2 = "skewedR_250K.txt";
    	String outFile = "spatiotextual_250K_Final_2.csv";
    	
    	sampleSize = (int) (samplePercentage * inputSize);
    	workers = new int[numberOfWorkers];
    	
    	lb = new LoadBalancer(numberOfWorkers);
		rr = new RoundRobin(numberOfWorkers);
    	
		SparkConf conf = new SparkConf().setMaster("local[" + numberOfWorkers + "]").setAppName("General Spatial Join");
    	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    	conf.registerKryoClasses(new Class<?>[] {QuadTree.class, Node.class, Point.class, 
        	Point[].class, Node[].class, MathUtils.class, SpatioTextualJoin.class, LoadBalanceApp.class });
        sc = new JavaSparkContext(conf);
        
        int file1LonIndex = 1; int file1LatIndex = 2; int file1Tag = 1;
		int file2LonIndex = 1; int file2LatIndex = 2; int file2Tag = 2; 
		int keywordsIndex = 3;
		
		String keywordsSeparator = ",";
		String separator = "|"; 
		int tagIndex = 0;
		
		stj = new SpatioTextualJoin(file1LonIndex, file1LatIndex, file1Tag,
    			file2LonIndex, file2LatIndex, file2Tag, keywordsIndex, keywordsSeparator,
    			separator, tagIndex);
    	
    	// Read files
    	if (args.length == 0) {
    		FILE_PATH = "C:/Users/user/OneDrive/Documents/SPADES Project/Datasets/";        	
        	localFilePath = "C:/Users/user/Desktop/";
    	} else {
    		FILE_PATH = args[0];
        	file1 = args[1];
        	file2 = args[2];
        	localFilePath = args[3];	
        	numberOfWorkers = Integer.parseInt(args[4]);
        	inputSize = Integer.parseInt(args[5]);
    	}
    	
    	String pathToCsv = FILE_PATH + file1 + "," + FILE_PATH + file2;
    	
    	csvWriter = new FileWriter(localFilePath + outFile);    	
    	addLabels(csvWriter);
    	
    	//double toSecondsFactor = Math.pow(10, 9);
    	//long resultTime = 0;    	
    	
    	Broadcast<SpatioTextualJoin> broadcastStj = sc.broadcast(stj);
    	JavaRDD<String> file = sc.textFile(pathToCsv);
    	
		// Iterate through experiment setups
    	points = stj.mapToPoints(file, broadcastStj);
    	
    	// Round Robin
    	quadTreeDuplicateTest(sampleSize / (5 * workers.length), 5);
    	quadTreeDuplicateTest(sampleSize / (10 * workers.length), 10);
    	quadTreeDuplicateTest(sampleSize / (20 * workers.length), 20);
    	
    	
    	// Round Robing, Radius QT
    	quadTreeTest(sampleSize / (5 * workers.length), 5);
    	quadTreeTest(sampleSize / (10 * workers.length), 10);
    	quadTreeTest(sampleSize / (20 * workers.length), 20);
    	
    	// LPT
    	lptNoRadiusTest((int) (sampleSize / (5 * workers.length)), 5);
    	lptNoRadiusTest((int) (sampleSize / (10 * workers.length)), 10);
    	lptNoRadiusTest((int) (sampleSize / (20 * workers.length)), 20);    
    	
    	// LPT, Radius QT
    	lptDuplicateTest((int) (sampleSize / (5 * workers.length)), 5);
    	lptDuplicateTest((int) (sampleSize / (10 * workers.length)), 10);
    	lptDuplicateTest((int) (sampleSize / (20 * workers.length)), 20);
    	
    	csvWriter.flush();
		csvWriter.close();
		sc.close();
    }
    
    // Function select an element base on index and return 
    // an element 
    public static String[] getRandomElements(ArrayList<String> list, 
                                          int totalItems) 
    { 
        Random rand = new Random(); 
        
        ArrayList<String> oldList = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
        	oldList.add(list.get(i));
        }
        
        String[] out = new String[totalItems];
        for (int i = 0; i < totalItems; i++) { 
  
            // take a random index between 0 to size  
            // of given List 
            int randomIndex = rand.nextInt(oldList.size()); 
  
            // add element in temporary list 
            out[i] = oldList.get(randomIndex); 
  
            // Remove selected element from original list 
            oldList.remove(randomIndex); 
        } 
        
        return out; 
    } 
    
    private static List<Tuple2<Integer, Double>> calculateSampleJoinSelectivity(Node node, double radius) {
    	ArrayList<Tuple2<Integer, Double>> pairs = new ArrayList<Tuple2<Integer, Double>>();
		if (node.hasChildrenQuadrants()) {
			Node[] children = node.getChildren();
			for (int i = 0; i < children.length; i++) {
				pairs.addAll(calculateSampleJoinSelectivity(children[i], radius));
			}
		} else {
			pairs.add(new Tuple2<Integer, Double>(node.getId(), stj.getSampleJoinSelectivity(node, radius)));
		}
		
		return pairs;
    }
     
    private static void addLabels(FileWriter fw) throws IOException {
    	fw.append("Structure,Input Size,Sample Size,Radius,Grid Size,Sample Input Per Leaf,Partitions,Duplicates,Min,Max,Mean,Std,Pairs,Time(s)\n");
    }
    
    private static void addLabelsJS(FileWriter fw) throws IOException {
    	fw.append("Partition,Actual,Estimated,Difference\n");
    }
    
    private static void addLabelsWorkers(FileWriter fw) throws IOException {
    	fw.append("Worker,Points\n");
    }
}
