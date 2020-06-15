package gr.ds.unipi.spades;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import gr.ds.unipi.qtree.MathUtils;
import gr.ds.unipi.qtree.Node;
import gr.ds.unipi.qtree.QuadTree;
import scala.Tuple2;
import gr.ds.unipi.qtree.Point;


/**
 * Hello world!
 *
 */
public class App 
{
	// File path of input files
	private static final String FILE_PATH = "F:\\OneDrive\\Documents\\SPADES Project\\Working Drafts\\Files\\"; //"hdfs://localhost:9000/user/test/";
	
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
    	String pathToCsv = FILE_PATH + "u_100k.txt," + FILE_PATH + "uw_100k.txt";
    	JavaRDD<String> file = sc.textFile(pathToCsv);
    	
    	// Read feature objects' categories
    	BufferedReader csvReader = new BufferedReader(new FileReader(FILE_PATH + "categories.txt"));
    	String row;
    	ArrayList<String> categories = new ArrayList<String>();
		while ((row = csvReader.readLine()) != null) {
			categories.add(row.trim());
    	}
    	csvReader.close();
    	
    	// Map lines to points
    	JavaRDD<Point> points = stj.mapToPoints(file, broadcastStj);
    	
    	// Quad tree and query parameters
    	double minX = 0;
    	double minY = 0;
    	double maxX = 10;
    	double maxY = 10;
    	int samplePointsPerLeaf = 100;
    	double samplePercentage = 0.01;
    	
    	Random rand = new Random();    	
    	String[] keywords = { categories.get(rand.nextInt(categories.size())) };    
    	double similarityScore = 0.5;
    	double radius = 2;
    	
    	
    	long startTime = System.nanoTime();
    	// Create quad tree (Global Indexing)
    	QuadTree qt = stj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, samplePercentage, points);
    	long elapsedTime1 = System.nanoTime() - startTime;  
    	
    	// Broadcast quad tree
    	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
    	
    	
    	startTime = System.nanoTime();
    	JavaPairRDD<Integer, Iterable<Point>> groupedPairs = stj.map(points, broadcastQuadTree, broadcastStj, radius);
    	JavaRDD<Tuple2<Point, Point>> out = stj.reduce(groupedPairs, broadcastStj, radius, similarityScore , keywords);
    	
    	// An action is needed for the whole process to start
    	System.out.println(out.count());
    	long elapsedTime2 = System.nanoTime() - startTime;  
    	
    	double toSecondsFactor = Math.pow(10, 9);
    	System.out.println("Quadtree create time " + elapsedTime1 / toSecondsFactor);
    	System.out.println("Result set execution time " + elapsedTime2 / toSecondsFactor);
    	
    	sc.close();
    }
}
