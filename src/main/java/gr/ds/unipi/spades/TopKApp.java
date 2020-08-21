package gr.ds.unipi.spades;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import gr.ds.unipi.spades.geometry.FeatureObject;
import gr.ds.unipi.spades.geometry.Point;
import gr.ds.unipi.spades.quadTree.Node;
import gr.ds.unipi.spades.quadTree.QuadTree;
import gr.ds.unipi.spades.queries.TopK;
import gr.ds.unipi.spades.regularGrid.RegularGrid;
import gr.ds.unipi.spades.util.MathUtils;
import scala.Tuple2;

public class TopKApp 
{
	// File path of input files
	//private static final String FILE_PATH = "C:\\Users\\user\\OneDrive\\Documents\\SPADES Project\\Datasets\\"; //"hdfs://localhost:9000/user/test/";
	
    public static void main( String[] args ) throws IOException
    {
    	// Initialize spark context
    	SparkConf conf = new SparkConf().setMaster("local").setAppName("Test Spark");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class<?>[] {QuadTree.class, Node.class, Point.class, 
        	Point[].class, Node[].class, MathUtils.class, TopK.class });
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // Constant parameters;
        
        int file1LonIndex = 1; int file1LatIndex = 2; int file1Tag = 1;
		int file2LonIndex = 1; int file2LatIndex = 2; int file2Tag = 2; int file2KeywordsIndex = 3;
		
		// NUMBER OF WORKERS AND LOAD BALANCER
		int numberOfWorkers = 12;
		LoadBalancer lb = new LoadBalancer(numberOfWorkers);
		
		// This MUST be a Regex
		String file2KeywordsSeparator = ",";
		
		String separator = "|"; 
		int tagIndex = 0;

    	TopK topK = new TopK(file1LonIndex, file1LatIndex, file1Tag,
    			file2LonIndex, file2LatIndex, file2Tag, file2KeywordsIndex, file2KeywordsSeparator,
    			separator, tagIndex);
    	
    	// Broadcast spatiotextualjoin object
    	Broadcast<TopK> broadcastTopK = sc.broadcast(topK);
    	
    	// Read files
    	String FILE_PATH;
    	String file1;
    	String file2;
    	String localFilePath;
    	if (args.length == 0) {
    		FILE_PATH = "C:/Users/user/OneDrive/Documents/SPADES Project/Datasets/";
        	file1 = "skewed_1M.txt";
        	file2 = "skewedK_1M.txt";
        	localFilePath = "C:/Users/user/OneDrive/Documents/SPADES Project/Datasets/";
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
    	int numberOfRunsPerFileSet = 40;
    	double radius = 2;
    	String dist = "Clustered";
    	String[] keywords;
    	
    	FileWriter csvWriter = new FileWriter("C:/Users/user/Desktop/all-in-all_2m.csv");    	
    	addLabels(csvWriter);
    	
    	double toSecondsFactor = Math.pow(10, 9);
    	int numberOfResults = 10;
    	long startTime = 0;
    	long indexCreationTime = 0;
    	long resultTime = 0;
    	// Iterate through experiment setups
    	for (int i = 0; i < numberOfRunsPerFileSet; i++) {
    		try {    		
    			topK.resetBins();
        		if (i == 0) {
        			radius = 2;
        		} else if (i == 20) {
        			radius = 5;
        		}
        		
        		if (i < 10 || (i >= 20 && i < 30)) {
        			keywords = getRandomElements(categories, 1);  
        		} else {
        			keywords = getRandomElements(categories, 3);
        		}
        		
        		// Map lines to points
            	JavaRDD<Point> points = topK.mapToPoints(file, broadcastTopK, keywords);
        		
        		// ------------ REGULAR GRID ---------------
            	
            	startTime = System.nanoTime();
            	// Create regular grid (Global Indexing)
            	RegularGrid grid = topK.createRegularGrid(minX, minY, maxX, maxY, hSectors, vSectors);
            	indexCreationTime = System.nanoTime() - startTime;  
            	
            	// Broadcast regular grid
            	Broadcast<RegularGrid> broadcastRegularGrid = sc.broadcast(grid);
            	
            	// Map points to cells
            	JavaPairRDD<Integer, List<Point>> groupedPairs = topK.map(points, broadcastRegularGrid, radius);
            	
            	startTime = System.nanoTime();
            	ArrayList<Tuple2<Point, Point>>  out = new ArrayList<Tuple2<Point, Point>>(topK.reduce(groupedPairs, radius, numberOfResults).collect());
            	out.sort(FeatureObject.PairComparator);
            	List<Tuple2<Point, Point>>  results = new ArrayList<Tuple2<Point, Point>>();
            	
            	for (int j = 0; j < numberOfResults; j++) {
            		results.add(out.get(j));
            	}
            	
            	resultTime = System.nanoTime() - startTime;
            	csvWriter.append("Regular Grid," + dist + "," + inputSize + "," + radius + "," + keywords.length + "," + hSectors + "x" + vSectors + ",," + 
            			indexCreationTime / toSecondsFactor + "," + resultTime / toSecondsFactor + "\n");
            	
        		// ------------ QUAD TREE (~ CONSTANT NUMBER OF LEAVES) ---------------
            	samplePercentage = 0.005;
            	samplePointsPerLeaf = 1;
        		startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	QuadTree qt = topK.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, samplePercentage, points);
            	indexCreationTime = System.nanoTime() - startTime;
            	
            	// Broadcast quad tree
            	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
            	
            	groupedPairs = topK.map(points, broadcastQuadTree, radius); 
            	
            	startTime = System.nanoTime();
            	out = new ArrayList<Tuple2<Point, Point>>(topK.reduceQuadTreeMBRCheck(groupedPairs, radius, numberOfResults).collect());
            	out.sort(FeatureObject.PairComparator);
            	results = new ArrayList<Tuple2<Point, Point>>();
            	for (int j = 0; j < numberOfResults; j++) {
            		results.add(out.get(j));
            	}
            	
        		resultTime = System.nanoTime() - startTime;  
        		
            	csvWriter.append("Quad Tree Constant Leaves," + dist + "," + inputSize + "," + radius + "," + keywords.length + ",," + samplePointsPerLeaf + "," + 
            			indexCreationTime / toSecondsFactor + "," + resultTime / toSecondsFactor + "\n");
            	
            	// --------------------- END QUAD TREE MBR CHECK ---------------------
            	
            	// --------------------- QUAD TREE LPT APPROXIMATION ---------------------
            	
            	startTime = System.nanoTime();
            	samplePercentage = 0.01;
            	samplePointsPerLeaf = (int) (inputSize * samplePercentage) / (5 * numberOfWorkers);
            	// Create quad tree (Global Indexing)
            	qt = topK.createQuadTreeLPT(minX, minY, maxX, maxY, samplePointsPerLeaf, samplePercentage, points);
            	
            	indexCreationTime = System.nanoTime() - startTime;
            	
            	groupedPairs = topK.map(points, broadcastQuadTree, radius);
            	lb.assignDataToReducer(topK.getBins());
            	groupedPairs = groupedPairs.partitionBy(lb);
            	startTime = System.nanoTime();
            	out = new ArrayList<Tuple2<Point, Point>>(topK.reduceQuadTreeMBRCheck(groupedPairs, radius, numberOfResults).collect());
            	out.sort(FeatureObject.PairComparator);
            	results = new ArrayList<Tuple2<Point, Point>>();
            	for (int j = 0; j < numberOfResults; j++) {
            		results.add(out.get(j));
            	}
            	
        		resultTime = System.nanoTime() - startTime;  
        		
            	csvWriter.append("Quad Tree LPT," + dist + "," + inputSize + "," + radius + "," + keywords.length + ",," + samplePointsPerLeaf + "," + 
            			indexCreationTime / toSecondsFactor + "," + resultTime / toSecondsFactor + "\n");
            	
            	// --------------------- END QUAD TREE NORMAL ---------------------
            	
            	// --------------------- QUAD TREE MBR CHECK WITH POINTS PER LEAF PROPORTIONAL TO NUMBER OF WORKERS (GeoSpark) ---------------------
            	
            	startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	samplePercentage = 0.01;
            	samplePointsPerLeaf = (int) (inputSize * samplePercentage) / numberOfWorkers;
            	qt = topK.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, samplePercentage, points);
            	indexCreationTime = System.nanoTime() - startTime;  
            	
            	// Broadcast quad tree
            	broadcastQuadTree = sc.broadcast(qt);
            	
            	groupedPairs = topK.map(points, broadcastQuadTree, radius);
            	
            	startTime = System.nanoTime();
            	out = new ArrayList<Tuple2<Point, Point>>(topK.reduceQuadTreeMBRCheck(groupedPairs, radius, numberOfResults).collect());
            	out.sort(FeatureObject.PairComparator);
            	results = new ArrayList<Tuple2<Point, Point>>();
            	for (int j = 0; j < numberOfResults; j++) {
            		results.add(out.get(j));
            	}
            	
        		resultTime = System.nanoTime() - startTime;  
        		
            	csvWriter.append("Quad Tree GeoSpark," + dist + "," + inputSize + "," + radius + "," + keywords.length + ",," + samplePointsPerLeaf + "," + 
            			indexCreationTime / toSecondsFactor + "," + resultTime / toSecondsFactor + "\n");
            	
            	// --------------------- END QUAD TREE MBR CHECK WITH POINTS PER LEAF PROPORTIONAL TO NUMBER OF WORKERS ---------------------
            	

            	           	
    		} catch (AssertionError ex) {
    			FileWriter errors = new FileWriter("C:/Users/user/Desktop/errors.csv", true);
    			errors.write(i + "\n");
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
    	fw.append("Structure,Distribution,Input Size,Radius,Query Keywords,Grid Size,Sample Input Per Leaf,Index Creation Time (s),Result Set Time (s)\n");
    }
}
