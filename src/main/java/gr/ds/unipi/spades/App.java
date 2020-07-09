package gr.ds.unipi.spades;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import gr.ds.unipi.spades.geometry.FeatureObject;
import gr.ds.unipi.spades.geometry.Point;
import gr.ds.unipi.spades.quadTree.Node;
import gr.ds.unipi.spades.quadTree.QuadTree;
import gr.ds.unipi.spades.regularGrid.RegularGrid;
import gr.ds.unipi.spades.util.MathUtils;
import scala.Tuple2;

public class App 
{
	// File path of input files
	//private static final String FILE_PATH = "C:\\Users\\user\\OneDrive\\Documents\\SPADES Project\\Datasets\\"; //"hdfs://localhost:9000/user/test/";
	
    public static void main( String[] args ) throws IOException
    {
    	// Initialize spark context
    	SparkConf conf = new SparkConf().setMaster("local").setAppName("Test Spark");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class<?>[] {QuadTree.class, Node.class, Point.class, 
        	Point[].class, Node[].class, MathUtils.class, SpatioTextualJoin.class });
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // Constant parameters;
        int file1LonIndex = 1; int file1LatIndex = 2; int file1Tag = 1;
		int file2LonIndex = 1; int file2LatIndex = 2; int file2Tag = 2; int file2KeywordsIndex = 3;
		
		// This MUST be a Regex
		String file2KeywordsSeparator = ",";
		
		String separator = "|"; int tagIndex = 0;

    	SpatioTextualJoin stj = new SpatioTextualJoin(file1LonIndex, file1LatIndex, file1Tag,
    			file2LonIndex, file2LatIndex, file2Tag, file2KeywordsIndex, file2KeywordsSeparator,
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
        	file1 = "s_1M.txt";
        	file2 = "sw_1M.txt";
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
    	double minX = -4;
    	double minY = -6;
    	double maxX = 4;
    	double maxY = 4;
    	int inputSize = 1_000_000;
    	int samplePointsPerLeaf = 1;
    	double samplePercentage = 0.01;
    	int hSectors = 100;
    	int vSectors = 100;    	   	 
    	int numberOfRunsPerFileSet = 20;
    	double radius = 2;
    	String dist = "Clustered";
    	String[] keywords;
    	
    	FileWriter csvWriter = new FileWriter("C:/Users/user/Desktop/experiments_s1M.csv");
    	
    	addLabels(csvWriter);
    	
    	int assertionErrors = 0;
    	long pairsQT = 0;
    	long pairsRG = 0;
    	long ppsj = 0;
    	long pJC = 0;
    	int numberOfResults = 10;
    	for (int i = 0; i < numberOfRunsPerFileSet; i++) {
    		try {
    			broadcastStj.getValue().resetCounts();
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
        		
        		FileWriter loadBalance = new FileWriter("C:/Users/user/Desktop/LoadBalance/countByKey_" + i + ".csv");
        		loadBalance.write("Structure,Leaf/Cell Id,Points\n");
        		// ------------ QUAD TREE ---------------
        		// Map lines to points
            	JavaRDD<Point> points = stj.mapToPoints(file, broadcastStj, keywords);
        		long startTime = System.nanoTime();
            	// Create quad tree (Global Indexing)
            	QuadTree qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, samplePercentage, points);
            	long elapsedTime1 = System.nanoTime() - startTime;  
            	
            	// Broadcast quad tree
            	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
            	
            	JavaPairRDD<Integer, Iterable<Point>> groupedPairs = stj.map(points, broadcastQuadTree, radius, keywords, broadcastStj);
            	
            	List<Tuple2<Integer, Integer>> counts = groupedPairs.mapValues(iter -> {
            		List<Point> pp = new ArrayList<Point>((Collection<? extends Point>) iter);
            		return pp.size();
            	}).collect();

            	for(Tuple2<Integer, Integer> pair : counts) {
            	    loadBalance.write("Quad Tree," + pair._1.intValue() + "," + pair._2.intValue() + "\n");
            	}
            	
            	startTime = System.nanoTime();
            	ArrayList<Tuple2<Point, Point>> out = new ArrayList<Tuple2<Point, Point>>(stj.reduce(groupedPairs, radius, numberOfResults, broadcastStj).collect());
            	out.sort(FeatureObject.PairComparator);
            	List<Tuple2<Point, Point>> results = new ArrayList<Tuple2<Point, Point>>();
            	for (int j = 0; j < numberOfResults; j++) {
            		results.add(out.get(j));
            	}
            	
        		pairsQT = results.size();
        		long elapsedTime2 = System.nanoTime() - startTime;  
        		double toSecondsFactor = Math.pow(10, 9);
            	csvWriter.append("Quad Tree," + dist + "," + inputSize + "," + radius + "," + keywords.length + ",," + samplePointsPerLeaf + ",[0-10],[0-10]," + 
            			elapsedTime1 / toSecondsFactor + "," + elapsedTime2 / toSecondsFactor + "," + broadcastStj.getValue().getHaversineCount() + ","
            			+ broadcastStj.getValue().getJaccardCount() + "," + broadcastStj.getValue().getPairsCount() + ",No\n");
            	
            	
            	
            	
            	// ------------ REGULAR GRID ---------------
            	
            	startTime = System.nanoTime();
            	// Create regular grid (Global Indexing)
            	RegularGrid grid = stj.createRegularGrid(minX, minY, maxX, maxY, hSectors, vSectors);
            	long elapsedTime3 = System.nanoTime() - startTime;  
            	
            	// Broadcast regular grid
            	Broadcast<RegularGrid> broadcastRegularGrid = sc.broadcast(grid);
            	
            	groupedPairs = stj.map(points, broadcastRegularGrid, radius, keywords, broadcastStj);
            	counts = groupedPairs.mapValues(iter -> {
            		List<Point> pp = new ArrayList<Point>((Collection<? extends Point>) iter);
            		return pp.size();
            	}).collect();

            	for(Tuple2<Integer, Integer> pair : counts) {
            	    loadBalance.write("Regular Grid," + pair._1.intValue() + "," + pair._2.intValue() + "\n");
            	}       	
            	
            	startTime = System.nanoTime();
            	out = new ArrayList<Tuple2<Point, Point>>(stj.reduce(groupedPairs, radius, numberOfResults, broadcastStj).collect());
            	out.sort(FeatureObject.PairComparator);
            	results = new ArrayList<Tuple2<Point, Point>>();
            	for (int j = 0; j < numberOfResults; j++) {
            		results.add(out.get(j));
            	}
            	
            	pairsRG = results.size();
            	long elapsedTime4 = System.nanoTime() - startTime;
            	csvWriter.append("Regular Grid," + dist + "," + inputSize + "," + radius + "," + keywords.length + "," + hSectors + "x" + vSectors + ",,[0-10],[0-10]," + 
            			elapsedTime3 / toSecondsFactor + "," + elapsedTime4 / toSecondsFactor + "," + broadcastStj.getValue().getHaversineCount() + ","
            			+ broadcastStj.getValue().getJaccardCount() + "," + broadcastStj.getValue().getPairsCount() + ",No\n");
            	
            	
            	loadBalance.write("QT Dupl," + stj.getQuadTreeDuplications() + ",RG Dupl," + stj.getRegularGridDuplications() + "\n");
            	
            	loadBalance.flush();
            	loadBalance.close();
            	
            	assert pairsQT == pairsRG;            	
    		} catch (AssertionError ex) {
    			assertionErrors++;
    			FileWriter errors = new FileWriter("C:/Users/user/Desktop/errors.csv", true);
    			errors.append(pairsQT + "," + pJC + "," + ppsj + "," + i + "," + assertionErrors + ",n1M\n");
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
    	fw.append("Structure,Distribution,Input Size,Radius,Query Keywords,Grid Size,Sample Input Per Leaf,X,Y,Index Creation Time (s),Result Set Time (s),Haversine Count,Jaccard Count,Pairs Count,Plane Sweep\n");
    }
}
