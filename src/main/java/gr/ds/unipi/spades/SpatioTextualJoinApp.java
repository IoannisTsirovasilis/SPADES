package gr.ds.unipi.spades;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import gr.ds.unipi.spades.geometry.DataObject;
import gr.ds.unipi.spades.geometry.FeatureObject;
import gr.ds.unipi.spades.geometry.Point;
import gr.ds.unipi.spades.quadTree.Node;
import gr.ds.unipi.spades.quadTree.QuadTree;
import gr.ds.unipi.spades.queries.SpatioTextualJoin;
import gr.ds.unipi.spades.queries.TopK;
import gr.ds.unipi.spades.regularGrid.RegularGrid;
import gr.ds.unipi.spades.util.MathUtils;
import scala.Tuple2;

public class SpatioTextualJoinApp {
	public static void main(String[] args) throws IOException
    {
    	// Initialize spark context
    	SparkConf conf = new SparkConf().setMaster("local").setAppName("Test Spark");
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
    	}
    	
    	String pathToCsv = FILE_PATH + file1 + "," + FILE_PATH + file2;
    	JavaRDD<String> file = sc.textFile(pathToCsv);
    	
    	// Read feature objects' categories
    	BufferedReader csvReader = new BufferedReader(new FileReader(localFilePath + "categories.txt"));
    	String row;
    	ArrayList<String> categories = new ArrayList<String>();
		while ((row = csvReader.readLine()) != null) {
			categories.add(row.trim());
    	}
    	csvReader.close();
    	
    	// Quad tree and query parameters
    	double minX = -15;
    	double minY = -12;
    	double maxX = 15;
    	double maxY = 8;
    	int inputSize = 1_000_000;
    	double samplePercentage = 0.01;
    	int samplePointsPerLeaf = (int) (inputSize * samplePercentage) / numberOfWorkers;    	
    	int hSectors = 100;
    	int vSectors = 100;    	   	 
    	int numberOfRunsPerFileSet = 6;
    	double radius = 2;
    	
    	FileWriter csvWriter = new FileWriter(localFilePath + "spatiotextual_250k.csv");    	
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
    			stj.resetBins();
        		if (i == 0) {
        			radius = 2;
        		} else if (i == 3) {
        			radius = 4;
        		}
        		
        		// Map lines to points
            	JavaRDD<Point> points = stj.mapToPoints(file, broadcastStj);
        		
        		// ------------ REGULAR GRID ---------------
            	
            	startTime = System.nanoTime();
            	// Create regular grid (Global Indexing)
            	RegularGrid grid = stj.createRegularGrid(minX, minY, maxX, maxY, hSectors, vSectors);
            	indexCreationTime = System.nanoTime() - startTime;  
            	
            	// Broadcast regular grid
            	Broadcast<RegularGrid> broadcastRegularGrid = sc.broadcast(grid);
            	
            	// Map points to cells
            	JavaPairRDD<Integer, Point> pairs = stj.map(points, broadcastRegularGrid, radius);
            	
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	
            	// Group By Key
            	JavaPairRDD<Integer, List<Point>> groupedPairs = pairs.groupByKey().mapValues(iter -> {
                	List<Point> pp = new ArrayList<Point>((Collection<? extends Point>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	partitions = groupedPairs.keys().count();
            	
            	startTime = System.nanoTime();
            	outPairs = stj.reduce(groupedPairs, radius).count();            	
            	resultTime = System.nanoTime() - startTime;
            	csvWriter.append("Regular Grid," + inputSize + ",," + radius + "," + hSectors + "x" + vSectors + ",," + 
            			partitions + "," + duplicates + "," + indexCreationTime / toSecondsFactor + "," + resultTime / toSecondsFactor + "\n");
            	
        		// ------------ QUAD TREE (~ CONSTANT NUMBER OF LEAVES) ---------------
            	samplePercentage = 0.01;
            	samplePointsPerLeaf = 1;
            	sampleSize = (int) (samplePercentage * inputSize);
        		startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	QuadTree qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
            	indexCreationTime = System.nanoTime() - startTime;
            	
            	// Broadcast quad tree
            	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
            	
            	// Map points to cells
            	pairs = stj.map(points, broadcastQuadTree, radius);
            	
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	
            	// Group By Key
            	groupedPairs = pairs.groupByKey().mapValues(iter -> {
                	List<Point> pp = new ArrayList<Point>((Collection<? extends Point>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	partitions = groupedPairs.keys().count();
            	
            	startTime = System.nanoTime();
            	tempPairs = stj.reduceQuadTreeMBRCheck(groupedPairs, radius).count();    
        		resultTime = System.nanoTime() - startTime;  
        		
        		assert outPairs == tempPairs;
        		
            	csvWriter.append("Quad Tree," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + indexCreationTime / toSecondsFactor + "," + resultTime / toSecondsFactor + "\n");
            	
            	// --------------------- END QUAD TREE MBR CHECK ---------------------
            	
            	// --------------------- QUAD TREE LPT APPROXIMATION ---------------------
            	
            	samplePercentage = 0.01;
            	sampleSize = (int) (samplePercentage * inputSize);
            	samplePointsPerLeaf = (int) (sampleSize / (5 * numberOfWorkers));
            	
            	startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	qt = stj.createQuadTreeLPT(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
            	
            	indexCreationTime = System.nanoTime() - startTime;
            	
            	// Map points to cells
            	pairs = stj.map(points, broadcastQuadTree, radius);
            	
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	
            	// Group By Key
            	groupedPairs = pairs.groupByKey().mapValues(iter -> {
                	List<Point> pp = new ArrayList<Point>((Collection<? extends Point>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	partitions = groupedPairs.keys().count();

            	lb.assignDataToReducer(stj.getBins());
            	groupedPairs = groupedPairs.partitionBy(lb);
            	
            	startTime = System.nanoTime();
            	tempPairs = stj.reduceQuadTreeMBRCheck(groupedPairs, radius).count();    
        		resultTime = System.nanoTime() - startTime;  
        		
        		assert outPairs == tempPairs;
            	
            	csvWriter.append("LPT," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + indexCreationTime / toSecondsFactor + "," + resultTime / toSecondsFactor + "\n");
            	
            	// --------------------- END QUAD TREE NORMAL ---------------------
            	
            	// --------------------- QUAD TREE MBR CHECK WITH POINTS PER LEAF PROPORTIONAL TO NUMBER OF WORKERS (GeoSpark) ---------------------
            	
            	startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	samplePercentage = 0.01;
            	sampleSize = (int) (inputSize * samplePercentage);
            	samplePointsPerLeaf =  (int) (sampleSize / numberOfWorkers);
            	qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, sampleSize, points);
            	indexCreationTime = System.nanoTime() - startTime;  
            	
            	// Broadcast quad tree
            	broadcastQuadTree = sc.broadcast(qt);
            	
            	// Map points to cells
            	pairs = stj.map(points, broadcastQuadTree, radius);
            	
            	// Calculate duplicates
            	duplicates = pairs.values().count() - inputSize; 
            	
            	// Group By Key
            	groupedPairs = pairs.groupByKey().mapValues(iter -> {
                	List<Point> pp = new ArrayList<Point>((Collection<? extends Point>) iter);
                	pp.sort(DataObject.Comparator);
                	return pp;
                });	// group by leaf id and sort values based on tag
            	
            	partitions = groupedPairs.keys().count();
            	
            	startTime = System.nanoTime();
            	tempPairs = stj.reduceQuadTreeMBRCheck(groupedPairs, radius).count();    
        		resultTime = System.nanoTime() - startTime;  
        		
        		assert outPairs == tempPairs;
        		
        		
            	csvWriter.append("GeoSpark," + inputSize + "," + sampleSize + "," + radius + ",," + samplePointsPerLeaf + "," + 
            			partitions + "," + duplicates + "," + indexCreationTime / toSecondsFactor + "," + resultTime / toSecondsFactor + "\n");
            	
            	// --------------------- END QUAD TREE MBR CHECK WITH POINTS PER LEAF PROPORTIONAL TO NUMBER OF WORKERS ---------------------
            	           	
    		} catch (AssertionError ex) {
    			FileWriter errors = new FileWriter(localFilePath + "errors.csv", true);
    			errors.write(i + "," + outPairs + "," + tempPairs + "\n");
    			errors.flush();
    	    	errors.close();
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
    	fw.append("Structure,Input Size,Sample Size,Radius,Grid Size,Sample Input Per Leaf,Partitions,Duplicates,Index Creation Time (s),Result Set Time (s)\n");
    }
}