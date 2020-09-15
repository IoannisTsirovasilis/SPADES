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
	
	public static void main(String[] args) throws IOException
    {
    	// Initialize spark context
		int numberOfWorkers = 10;
		int inputSize = 250_000;
		SparkConf conf = new SparkConf().setMaster("local[" + numberOfWorkers + "]").setAppName("General Spatial Join");
    	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    	conf.registerKryoClasses(new Class<?>[] {QuadTree.class, Node.class, Point.class, 
        	Point[].class, Node[].class, MathUtils.class, SpatioTextualJoin.class, LoadBalanceApp.class });
        JavaSparkContext sc = new JavaSparkContext(conf);
		
		
        // Constant parameters
        
        int file1LonIndex = 1; int file1LatIndex = 2; int file1Tag = 1;
		int file2LonIndex = 1; int file2LatIndex = 2; int file2Tag = 2; 
		int keywordsIndex = 3;
		// This MUST be a Regex
		String keywordsSeparator = ",";
		String separator = "|"; 
		int tagIndex = 0;
		
		// NUMBER OF WORKERS AND LOAD BALANCER
		
		LoadBalancer lb = new LoadBalancer(numberOfWorkers);
		RoundRobin rr = new RoundRobin(numberOfWorkers);
		
		SpatioTextualJoin stj = new SpatioTextualJoin(file1LonIndex, file1LatIndex, file1Tag,
    			file2LonIndex, file2LatIndex, file2Tag, keywordsIndex, keywordsSeparator,
    			separator, tagIndex);
    	
    	
    	
    	// Read files
    	String FILE_PATH;
    	String file1;
    	String file2;
    	String localFilePath;
    	if (args.length == 0) {
    		FILE_PATH = "C:/Users/user/OneDrive/Documents/SPADES Project/Datasets/";
        	file1 = "skewedL_250K.txt";
        	file2 = "skewedR_250K.txt";
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
    	
    	
    	// Quad tree and query parameters
    	double minX = -15;
    	double minY = -12;
    	double maxX = 15;
    	double maxY = 8;
    	
    	double samplePercentage = 0.1; 
    	int numberOfRunsPerFileSet = 6;
    	double radius = 2;
    	
    	FileWriter csvWriter = new FileWriter(localFilePath + "spatiotextual2.csv");    	
    	addLabels(csvWriter);
    	
    	//double toSecondsFactor = Math.pow(10, 9);
    	//long resultTime = 0;
    	int sampleSize = (int) (samplePercentage * inputSize);
    	int[] workers = new int[numberOfWorkers];
    	long duplicates;
    	long partitions;
    	int samplePointsPerLeaf;
    	// Iterate through experiment setups
    	for (int i = 0; i < numberOfRunsPerFileSet; i++) {
    		try {
    			System.out.println((i + 1) + " iteration...");
    	        // Broadcast spatiotextualjoin object
    	    	Broadcast<SpatioTextualJoin> broadcastStj = sc.broadcast(stj);
    	    	JavaRDD<String> file = sc.textFile(pathToCsv);	  
        		if (i == 0) {
        			radius = 2;
        		} else if (i == 3) {
        			radius = 4;
        		}  	
        		
        		// Map lines to points
        		JavaRDD<FeatureObject> points = stj.mapToPoints(file, broadcastStj);
        		
        		// ------------ REGULAR GRID ---------------
        		for (int c = 0; c < workers.length; c++) workers[c] = 0;
            	System.out.println("Creating regular grid...");
            	long startTime = System.nanoTime();
            	// Create regular grid (Global Indexing)
            	int hSectors = 100;
            	int vSectors = 100;
            	RegularGrid grid = stj.createRegularGrid(minX, minY, maxX, maxY, hSectors, vSectors);
            	long indexCreationTime = System.nanoTime() - startTime;  
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
            	int rgBalanceIndex = 0;
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
            	
            	csvWriter.append("Regular Grid," + inputSize + ",," + radius + "," + hSectors + "x" + vSectors + ",," + 
        				partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
        				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "\n");            	
            	
        		// ------------ QUAD TREE (~ CONSTANT NUMBER OF LEAVES) ---------------
            	
            	for (int c = 0; c < workers.length; c++) workers[c] = 0;
            	samplePointsPerLeaf = 1;
            	stj.resetBins();
            	System.out.println("Creating quad tree...");
        		startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	QuadTree qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
            	indexCreationTime = System.nanoTime() - startTime;
            	System.out.println("Quad tree created...");
            	rr.assignDataToReducer(stj.getBins());
            	// Broadcast quad tree
            	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
            	// Map points to cells
            	pairs = stj.map(points, broadcastQuadTree, radius);            	
            	
            	// Group By Key
            	groupedPairs = pairs.groupByKey(rr).mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	counts = groupedPairs.mapValues(iter -> {
                    List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                    return pp.size();
                }).collect();     
                
                iterator = counts.iterator();
            	while (iterator.hasNext()) {
            		n = iterator.next();
            		load = rr.loads.get(n._1);
        			workers[load] +=  n._2;
            	}
            	
        		System.out.println("Counting duplicates (QT)...");
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	System.out.println("Duplicates counted (QT)...");
            	
            	System.out.println("Counting partitions (QT)...");
            	partitions = groupedPairs.keys().count();
            	System.out.println("Partitions counted (QT)...");
        		
            	csvWriter.append("Quad Tree," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
        				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "\n");
        		// --------------------- END QUAD TREE MBR CHECK ---------------------
            	
            	// --------------------- QUAD TREE LPT APPROXIMATION ---------------------
            	
            	for (int c = 0; c < workers.length; c++) workers[c] = 0;
            	stj.resetBins();
            	samplePointsPerLeaf = (int) (sampleSize / (5 * workers.length));
            	System.out.println("Creating quad tree (LPT)...");
            	startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	qt = stj.createQuadTreeLPT(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
            	System.out.println("Quad tree created (LPT)...");
            	indexCreationTime = System.nanoTime() - startTime;
            	lb.assignDataToReducer(stj.getBins());
            	// Broadcast quad tree
            	broadcastQuadTree = sc.broadcast(qt);
            	
            	// Map points to cells
            	pairs = stj.map(points, broadcastQuadTree, radius);            	
            	
            	// Group By Key
            	groupedPairs = pairs.groupByKey(lb).mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	counts = groupedPairs.mapValues(iter -> {
                    List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                    return pp.size();
                }).collect();     
                
                iterator = counts.iterator();
            	while (iterator.hasNext()) {
            		n = iterator.next();
            		load = lb.loads.get(n._1);
        			workers[load] +=  n._2;
            	}

        		System.out.println("Counting duplicates (LPT)...");
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	System.out.println("Duplicates counted (LPT)...");
            	
            	System.out.println("Counting partitions (LPT)...");
            	partitions = groupedPairs.keys().count();
            	System.out.println("Partitions counted (LPT)...");            	
            	
            	csvWriter.append("LPT," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
        				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "\n");
            	
            	// --------------------- END QUAD TREE NORMAL ---------------------
            	
            	// --------------------- QUAD TREE MBR CHECK WITH POINTS PER LEAF PROPORTIONAL TO NUMBER OF WORKERS (GeoSpark) ---------------------
            	
            	for (int c = 0; c < workers.length; c++) workers[c] = 0;
            	stj.resetBins();
            	System.out.println("Creating quad tree (GS)...");
            	startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	samplePointsPerLeaf = (int) (sampleSize / workers.length);
            	qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
            	indexCreationTime = System.nanoTime() - startTime;  
            	System.out.println("Quad tree created (GS)...");
            	rr.assignDataToReducer(stj.getBins());
            	// Broadcast quad tree
            	broadcastQuadTree = sc.broadcast(qt);
            	
            	// Map points to cells
            	pairs = stj.map(points, broadcastQuadTree, radius);            	
            	
            	// Group By Key
            	groupedPairs = pairs.groupByKey(rr).mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	counts = groupedPairs.mapValues(iter -> {
                    List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                    return pp.size();
                }).collect();     
                
                iterator = counts.iterator();
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
            
            	// --------------------- END QUAD TREE MBR CHECK WITH POINTS PER LEAF PROPORTIONAL TO NUMBER OF WORKERS ---------------------
            	            	           	
    		} catch (AssertionError ex) {
    			FileWriter errors = new FileWriter(localFilePath + "errors.csv", true);
    			errors.write(i + "\n");
    			errors.flush();
    	    	errors.close();
    		} finally {
    			Toolkit.getDefaultToolkit().beep();
    			
    		}
    	}
    	
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
    
    private static void addLabels(FileWriter fw) throws IOException {
    	fw.append("Structure,Input Size,Sample Size,Radius,Grid Size,Sample Input Per Leaf,Partitions,Duplicates,Min,Max,Mean,Std\n");
    }
    
    private static void geosparkTest(double minX, double minY, double maxX, double maxY,
    		double samplePercentage, int sampleSize, double radius, JavaRDD<FeatureObject> points,
    		JavaSparkContext sc, SpatioTextualJoin stj, RoundRobin rr, FileWriter csvWriter,
    		JavaPairRDD<Integer, FeatureObject> pairs, JavaPairRDD<Integer, List<FeatureObject>> groupedPairs,
    		List<Tuple2<Integer, Integer>> counts, int[] workers, int inputSize) throws IOException {
    	for (int c = 0; c < workers.length; c++) workers[c] = 0;
    	stj.resetBins();
    	System.out.println("Creating quad tree (GS)...");
    	long startTime = System.nanoTime();
    	// Create quad tree (Global Indexing)
    	int samplePointsPerLeaf = (int) (sampleSize / workers.length);
    	QuadTree qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
    	long indexCreationTime = System.nanoTime() - startTime;  
    	System.out.println("Quad tree created (GS)...");
    	rr.assignDataToReducer(stj.getBins());
    	// Broadcast quad tree
    	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
    	
    	// Map points to cells
    	pairs = stj.map(points, broadcastQuadTree, radius);            	
    	
    	// Group By Key
    	groupedPairs = pairs.groupByKey(rr).mapValues(iter -> {
        	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
        	pp.sort(DataObject.Comparator);
        	return pp;
        });	// group by leaf id and sort values based on tag
    	
    	counts = groupedPairs.mapValues(iter -> {
            List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
            return pp.size();
        }).collect();     
        
        Iterator<Tuple2<Integer, Integer>> iterator = counts.iterator();
    	Tuple2<Integer, Integer> n;
    	Integer load;
    	while (iterator.hasNext()) {
    		n = iterator.next();
    		load = rr.loads.get(n._1);
			workers[load] +=  n._2;
    	}
    	
		System.out.println("Counting duplicates (GS)...");
    	// Calculate duplicates
    	long duplicates = pairs.values().count() - inputSize; 
    	System.out.println("Duplicates counted (GS)...");
    	
    	System.out.println("Counting partitions (GS)...");
    	long partitions = groupedPairs.keys().count();
    	System.out.println("Partitions counted (GS)...");
		
    	csvWriter.append("GeoSpark," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
    			partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "\n");
    }
    
    private static void lptTest(double minX, double minY, double maxX, double maxY,
    		double samplePercentage, int sampleSize, double radius, JavaRDD<FeatureObject> points,
    		JavaSparkContext sc, SpatioTextualJoin stj, LoadBalancer lb, FileWriter csvWriter,
    		JavaPairRDD<Integer, FeatureObject> pairs, JavaPairRDD<Integer, List<FeatureObject>> groupedPairs,
    		List<Tuple2<Integer, Integer>> counts, int[] workers, int inputSize) throws IOException {
    	for (int c = 0; c < workers.length; c++) workers[c] = 0;
    	stj.resetBins();
    	int samplePointsPerLeaf = (int) (sampleSize / (5 * workers.length));
    	System.out.println("Creating quad tree (LPT)...");
    	long startTime = System.nanoTime();
    	// Create quad tree (Global Indexing)
    	QuadTree qt = stj.createQuadTreeLPT(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
    	System.out.println("Quad tree created (LPT)...");
    	long indexCreationTime = System.nanoTime() - startTime;
    	lb.assignDataToReducer(stj.getBins());
    	// Broadcast quad tree
    	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
    	
    	// Map points to cells
    	pairs = stj.map(points, broadcastQuadTree, radius);            	
    	
    	// Group By Key
    	groupedPairs = pairs.groupByKey(lb).mapValues(iter -> {
        	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
        	pp.sort(DataObject.Comparator);
        	return pp;
        });	// group by leaf id and sort values based on tag
    	
    	counts = groupedPairs.mapValues(iter -> {
            List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
            return pp.size();
        }).collect();     
        
        Iterator<Tuple2<Integer, Integer>> iterator = counts.iterator();
    	Tuple2<Integer, Integer> n;
    	Integer load;
    	while (iterator.hasNext()) {
    		n = iterator.next();
    		load = lb.loads.get(n._1);
			workers[load] +=  n._2;
    	}

		System.out.println("Counting duplicates (LPT)...");
    	// Calculate duplicates
    	long duplicates = pairs.values().count() - inputSize; 
    	System.out.println("Duplicates counted (LPT)...");
    	
    	System.out.println("Counting partitions (LPT)...");
    	long partitions = groupedPairs.keys().count();
    	System.out.println("Partitions counted (LPT)...");            	
    	
    	csvWriter.append("LPT," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
    			partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "\n");
    }
    
    private static void quadTreeTest(double minX, double minY, double maxX, double maxY,
    		double samplePercentage, int sampleSize, int samplePointsPerLeaf, double radius, JavaRDD<FeatureObject> points,
    		JavaSparkContext sc, SpatioTextualJoin stj, RoundRobin rr, FileWriter csvWriter,
    		JavaPairRDD<Integer, FeatureObject> pairs, JavaPairRDD<Integer, List<FeatureObject>> groupedPairs,
    		List<Tuple2<Integer, Integer>> counts, int[] workers, int inputSize) throws IOException {
    	for (int c = 0; c < workers.length; c++) workers[c] = 0;
    	stj.resetBins();
    	System.out.println("Creating quad tree...");
		long startTime = System.nanoTime();
    	// Create quad tree (Global Indexing)
    	QuadTree qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
    	long indexCreationTime = System.nanoTime() - startTime;
    	System.out.println("Quad tree created...");
    	rr.assignDataToReducer(stj.getBins());
    	// Broadcast quad tree
    	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
    	// Map points to cells
    	pairs = stj.map(points, broadcastQuadTree, radius);            	
    	
    	// Group By Key
    	groupedPairs = pairs.groupByKey(rr).mapValues(iter -> {
        	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
        	pp.sort(DataObject.Comparator);
        	return pp;
        });	// group by leaf id and sort values based on tag
    	
    	counts = groupedPairs.mapValues(iter -> {
            List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
            return pp.size();
        }).collect();     
        
        Iterator<Tuple2<Integer, Integer>> iterator = counts.iterator();
    	Tuple2<Integer, Integer> n;
    	Integer load;
    	while (iterator.hasNext()) {
    		n = iterator.next();
    		load = rr.loads.get(n._1);
			workers[load] +=  n._2;
    	}
    	
		System.out.println("Counting duplicates (QT)...");
    	// Calculate duplicates
    	long duplicates = pairs.values().count() - inputSize; 
    	System.out.println("Duplicates counted (QT)...");
    	
    	System.out.println("Counting partitions (QT)...");
    	long partitions = groupedPairs.keys().count();
    	System.out.println("Partitions counted (QT)...");
		
    	csvWriter.append("Quad Tree," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
    			partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "\n");
    }
    
    private static void regularGridTest(double minX, double minY, double maxX, double maxY,
    		int hSectors, int vSectors, double radius, JavaRDD<FeatureObject> points,
    		JavaSparkContext sc, SpatioTextualJoin stj, RoundRobin rr, FileWriter csvWriter,
    		JavaPairRDD<Integer, FeatureObject> pairs, JavaPairRDD<Integer, List<FeatureObject>> groupedPairs,
    		List<Tuple2<Integer, Integer>> counts, int[] workers, int inputSize) throws IOException {
    	for (int c = 0; c < workers.length; c++) workers[c] = 0;
    	System.out.println("Creating regular grid...");
    	long startTime = System.nanoTime();
    	// Create regular grid (Global Indexing)
    	RegularGrid grid = stj.createRegularGrid(minX, minY, maxX, maxY, hSectors, vSectors);
    	long indexCreationTime = System.nanoTime() - startTime;  
    	System.out.println("Regular grid created...");
    	// Broadcast regular grid
    	Broadcast<RegularGrid> broadcastRegularGrid = sc.broadcast(grid);
    	
    	// Map points to cells
    	pairs = stj.map(points, broadcastRegularGrid, radius);               
    	
    	// Group By Key
    	groupedPairs = pairs.groupByKey().mapValues(iter -> {
        	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
        	pp.sort(DataObject.Comparator);
        	return pp;
        });	// group by leaf id and sort values based on tag
    	
        counts = groupedPairs.mapValues(iter -> {
            List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
            return pp.size();
        }).collect();     
        
        Iterator<Tuple2<Integer, Integer>> iterator = counts.iterator();
    	Tuple2<Integer, Integer> n;
    	Integer load;
    	int rgBalanceIndex = 0;
    	while (iterator.hasNext()) {
    		n = iterator.next();
    		load = rgBalanceIndex++ % workers.length;
			workers[load] +=  n._2;
    	}
       
    	
    	// Calculate duplicates
    	System.out.println("Counting duplicates (RG)...");
    	long duplicates = pairs.count() - inputSize;
    	System.out.println("Duplicates counted (RG)...");
    	
    	System.out.println("Counting partitions (RG)");            	
    	long partitions = groupedPairs.keys().count();
    	System.out.println("Partitions counted (RG)...");
    	
    	csvWriter.append("Regular Grid," + inputSize + ",," + radius + "," + hSectors + "x" + vSectors + ",," + 
				partitions + "," + duplicates + "," + IntArrays.min(workers) + "," + IntArrays.max(workers)
				+ "," + IntArrays.mean(workers) + "," + IntArrays.std(workers) + "\n");
    }
}
