package gr.ds.unipi.spades;

import java.awt.Toolkit;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import gr.ds.unipi.spades.geometry.DataObject;
import gr.ds.unipi.spades.geometry.FeatureObject;
import gr.ds.unipi.spades.geometry.Point;
import gr.ds.unipi.spades.quadTree.Node;
import gr.ds.unipi.spades.quadTree.QuadTree;
import gr.ds.unipi.spades.queries.SpatioTextualJoin;
import gr.ds.unipi.spades.regularGrid.RegularGrid;
import gr.ds.unipi.spades.util.MathUtils;

public class SpatioTextualJoinApp {
	public static void main(String[] args) throws IOException
    {
    	// Initialize spark context
    	SparkConf conf = new SparkConf().setAppName("General Spatial Join");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class<?>[] {QuadTree.class, Node.class, Point.class, 
        	Point[].class, Node[].class, MathUtils.class, SpatioTextualJoin.class });
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
		int numberOfWorkers = 12;
		LoadBalancer lb = new LoadBalancer(numberOfWorkers);
		
    	SpatioTextualJoin stj = new SpatioTextualJoin(file1LonIndex, file1LatIndex, file1Tag,
    			file2LonIndex, file2LatIndex, file2Tag, keywordsIndex, keywordsSeparator,
    			separator, tagIndex);
    	
    	// Broadcast spatiotextualjoin object
    	Broadcast<SpatioTextualJoin> broadcastStj = sc.broadcast(stj);
    	
    	// Read files
    	String FILE_PATH;
    	String file1;
    	String file2;
    	String localFilePath;
    	int inputSize = 250_000;
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
    	JavaRDD<String> file = sc.textFile(pathToCsv);
    	
    	// Quad tree and query parameters
    	double minX = -15;
    	double minY = -12;
    	double maxX = 15;
    	double maxY = 8;
    	
    	double samplePercentage = 0.1;
    	int samplePointsPerLeaf = (int) (inputSize * samplePercentage) / numberOfWorkers;    	
    	int hSectors = 100;
    	int vSectors = 100;    	   	 
    	int numberOfRunsPerFileSet = 6;
    	double radius = 2;
    	
    	FileWriter csvWriter = new FileWriter(localFilePath + "spatiotextual.csv");    	
    	addLabels(csvWriter);
    	
    	double toSecondsFactor = Math.pow(10, 9);
    	long startTime = 0;
    	long indexCreationTime = 0;
    	long resultTime = 0;
    	
    	long duplicates = 0;
    	long partitions = 0;
    	long outPairs = 0;
    	long tempPairs = 0;
    	int sampleSize = 0;
    	
    	// Iterate through experiment setups
    	for (int i = 0; i < numberOfRunsPerFileSet; i++) {
    		try {
    			System.out.println((i + 1) + " iteration...");
    			stj.resetBins();
        		if (i == 0) {
        			radius = 2;
        		} else if (i == 3) {
        			radius = 4;
        		}  	
        		// Map lines to points
            	JavaRDD<FeatureObject> points = stj.mapToPoints(file, broadcastStj);
        		
        		// ------------ REGULAR GRID ---------------
            	
            	System.out.println("Creating regular grid...");
            	startTime = System.nanoTime();
            	// Create regular grid (Global Indexing)
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
            	
            	// Calculate duplicates
            	System.out.println("Counting duplicates (RG)...");
            	duplicates = pairs.count() - inputSize;
            	System.out.println("Duplicates counted (RG)...");
            	
            	System.out.println("Counting partitions (RG)");            	
            	partitions = groupedPairs.keys().count();
            	System.out.println("Partitions counted (RG)...");
            	
            	System.out.println("Counting result pairs (RG)");
            	startTime = System.nanoTime();
            	outPairs = stj.reduce(groupedPairs, radius).count();            	
            	resultTime = System.nanoTime() - startTime;
            	
            	System.out.println("Counting duplicates (RG)...");
            	
            	
            	csvWriter.append("Regular Grid," + inputSize + ",," + radius + "," + hSectors + "x" + vSectors + ",," + 
        				partitions + "," + duplicates + "," + outPairs + "," + indexCreationTime / toSecondsFactor + "," + resultTime / toSecondsFactor + "\n");
            	
        		// ------------ QUAD TREE (~ CONSTANT NUMBER OF LEAVES) ---------------
            	samplePercentage = 0.01;
            	samplePointsPerLeaf = 1;
            	sampleSize = (int) (samplePercentage * inputSize);
            	System.out.println("Creating quad tree...");
        		startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	QuadTree qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
            	indexCreationTime = System.nanoTime() - startTime;
            	System.out.println("Quad tree created...");
            	// Broadcast quad tree
            	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
            	
            	// Map points to cells
            	pairs = stj.map(points, broadcastQuadTree, radius);
            	
            	
            	// Group By Key
            	groupedPairs = pairs.groupByKey().mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	
            	System.out.println("Counting result pairs (QT)...");
            	startTime = System.nanoTime();
            	tempPairs = stj.reduce(groupedPairs, radius).count();    
        		resultTime = System.nanoTime() - startTime;  
        		
        		//assert outPairs == tempPairs;
        		
        		System.out.println("Counting duplicates (QT)...");
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	System.out.println("Duplicates counted (QT)...");
            	
            	System.out.println("Counting partitions (QT)...");
            	partitions = groupedPairs.keys().count();
            	System.out.println("Partitions counted (QT)...");
        		
            	csvWriter.append("Quad Tree," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + tempPairs + "," + indexCreationTime / toSecondsFactor + "," + resultTime / toSecondsFactor + "\n");
            	
            	// --------------------- END QUAD TREE MBR CHECK ---------------------
            	
            	// --------------------- QUAD TREE LPT APPROXIMATION ---------------------
            	
            	samplePercentage = 0.01;
            	sampleSize = (int) (samplePercentage * inputSize);
            	samplePointsPerLeaf = (int) (sampleSize / (5 * numberOfWorkers));
            	System.out.println("Creating quad tree (LPT)...");
            	startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	qt = stj.createQuadTreeLPT(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
            	System.out.println("Quad tree created (LPT)...");
            	indexCreationTime = System.nanoTime() - startTime;
            	
            	// Broadcast quad tree
            	broadcastQuadTree = sc.broadcast(qt);
            	
            	// Map points to cells
            	pairs = stj.map(points, broadcastQuadTree, radius);
            	
            	// Group By Key
            	groupedPairs = pairs.groupByKey().mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	System.out.println("Counting result pairs (LPT)...");
            	lb.assignDataToReducer(stj.getBins());
            	groupedPairs = groupedPairs.partitionBy(lb);
            	
            	startTime = System.nanoTime();
            	tempPairs = stj.reduce(groupedPairs, radius).count();    
        		resultTime = System.nanoTime() - startTime;  
        		
        		//assert outPairs == tempPairs;
            	
        		System.out.println("Counting duplicates (LPT)...");
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	System.out.println("Duplicates counted (LPT)...");
            	
            	System.out.println("Counting partitions (LPT)...");
            	partitions = groupedPairs.keys().count();
            	System.out.println("Partitions counted (LPT)...");
        		
            	csvWriter.append("LPT," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + tempPairs + "," + indexCreationTime / toSecondsFactor + "," + resultTime / toSecondsFactor + "\n");
            	
            	// --------------------- END QUAD TREE NORMAL ---------------------
            	
            	// --------------------- QUAD TREE MBR CHECK WITH POINTS PER LEAF PROPORTIONAL TO NUMBER OF WORKERS (GeoSpark) ---------------------
            	System.out.println("Creating quad tree (GS)...");
            	startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	samplePercentage = 0.01;
            	sampleSize = (int) (inputSize * samplePercentage);
            	samplePointsPerLeaf =  (int) (sampleSize / numberOfWorkers);
            	qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
            	indexCreationTime = System.nanoTime() - startTime;  
            	System.out.println("Quad tree created (GS)...");
            	// Broadcast quad tree
            	broadcastQuadTree = sc.broadcast(qt);
            	
            	// Map points to cells
            	pairs = stj.map(points, broadcastQuadTree, radius);
            	
            	// Group By Key
            	groupedPairs = pairs.groupByKey().mapValues(iter -> {
                	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	System.out.println("Counting result pairs (GS)...");
            	startTime = System.nanoTime();
            	tempPairs = stj.reduce(groupedPairs, radius).count();    
        		resultTime = System.nanoTime() - startTime;  
        		
        		//assert outPairs == tempPairs;
        		
        		System.out.println("Counting duplicates (GS)...");
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	System.out.println("Duplicates counted (GS)...");
            	
            	System.out.println("Counting partitions (GS)...");
            	partitions = groupedPairs.keys().count();
            	System.out.println("Partitions counted (GS)...");
        		
            	csvWriter.append("GeoSpark," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + tempPairs + "," + indexCreationTime / toSecondsFactor + "," + resultTime / toSecondsFactor + "\n");
            	
            	// --------------------- END QUAD TREE MBR CHECK WITH POINTS PER LEAF PROPORTIONAL TO NUMBER OF WORKERS ---------------------
            	           	
    		} catch (AssertionError ex) {
    			FileWriter errors = new FileWriter(localFilePath + "errors.csv", true);
    			errors.write(i + "," + outPairs + "," + tempPairs + "\n");
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
    	fw.append("Structure,Input Size,Sample Size,Radius,Grid Size,Sample Input Per Leaf,Partitions,Duplicates,Output Pairs,Index Creation Time (s),Result Set Time (s)\n");
    }
}