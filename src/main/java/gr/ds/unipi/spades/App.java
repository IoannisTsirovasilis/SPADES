package gr.ds.unipi.spades;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import gr.ds.unipi.qtree.Node;
import gr.ds.unipi.qtree.QuadTree;
import scala.Tuple2;
import gr.ds.unipi.qtree.Point;
import gr.ds.unipi.qtree.NodePointPair;
import org.apache.spark.storage.StorageLevel;


/**
 * Hello world!
 *
 */
public class App 
{
	private static final String FILE_PATH = "F:\\OneDrive\\Documents\\SPADES Project\\Working Drafts\\Files\\"; //"hdfs://localhost:9000/user/test/";
    public static void main( String[] args ) throws IOException
    {   	
    	// Initialize spark context
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Test Spark");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class<?>[] {QuadTree.class, Node.class, Point.class, Point[].class, Node[].class, NodePointPair.class});
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // Constant parameters
        int lonIndex = 0;
    	int latIndex = 1;
    	String separator = ",";
    	
    	double samplePecentage = 0.01;
    	double epsilon = 0.000001;
    	int pointsPerLeaf = 1_000;
    	int samplePointsPerLeaf = (int) (pointsPerLeaf * samplePecentage);
    	String fileName = "u1M.csv";
    	double minX = -124.586307, minY = -42.118331, maxX = 47.669728, maxY = 49.00106;
    	minX = -300; minY = -300; maxX = 300; maxY = 300;
        
        // Read sample file
        System.out.println("Reading file...");
        JavaRDD<String> lines = sc.textFile(FILE_PATH + fileName);
        lines = lines.filter(line -> !line.contains("x"));
        System.out.println("File read.");
        
        // Create (empty) quad tree at MASTER node
        QuadTree quadTree = new QuadTree(minX - epsilon, minY - epsilon, maxX + epsilon, maxY + epsilon, samplePointsPerLeaf);
        
        // Map
        // Extract point information
        JavaRDD<Point> points = lines.map(line -> {
        	String lon, lat;
        	int lonOrdinalIndex, latOrdinalIndex;
        	if (lonIndex == 0) {
    			lon = line.substring(0, line.indexOf(separator));
    		} else {
    			lonOrdinalIndex = StringUtils.ordinalIndexOf(line, separator, lonIndex);
    			
    			if (lonOrdinalIndex == StringUtils.lastOrdinalIndexOf(line, separator, 1))
        		{
        			lon =  line.substring(lonOrdinalIndex + 1);
        		} else {
        			lon = line.substring(lonOrdinalIndex + 1, StringUtils.ordinalIndexOf(line, separator, lonIndex + 1));
        		}    			
    		}
    		
    		
        	if (latIndex == 0) {
    			lat = line.substring(0, line.indexOf(separator));
    		} else {
    			latOrdinalIndex = StringUtils.ordinalIndexOf(line, separator, latIndex);
    			
    			if (latOrdinalIndex == StringUtils.lastOrdinalIndexOf(line, separator, 1))
        		{
        			lat =  line.substring(latOrdinalIndex + 1);
        		} else {
        			lat = line.substring(latOrdinalIndex + 1, StringUtils.ordinalIndexOf(line, separator, latIndex + 1));
        		}
    		}
        	
    		return new Point(Double.parseDouble(lon), Double.parseDouble(lat));         	
        });
        
        
        // ************* GLOBAL INDEXING PHASE *************        
        // Insert a sample of the data set (1% of total size) into the quad tree
        
        // sampling
        List<Point> sample = points.takeSample(false, (int) (points.count() * samplePecentage));
        
        long startTime = System.nanoTime();
        for (Point p : sample) {
        	quadTree.insertPoint(p);
        }       
        long elapsedTime = System.nanoTime() - startTime;        
     
        quadTree.traverse();
        
        System.out.println("Total execution time (ms): " + elapsedTime / 1000000);
        System.out.println("Number of contained points of Tree " + quadTree.getNumberOfInsertedPoints());
        System.out.println(String.format("Number of Leaves: %d", quadTree.getNumberOfLeaves()));
        System.out.println(String.format("Number of Full Leaves: %d", quadTree.getNumberOfFullLeaves()));
        System.out.println(String.format("Number of Empty Leaves: %d", quadTree.getNumberOfEmptyLeaves()));
        System.out.println(String.format("Points per Leaf: %f", quadTree.getMeanNumberOfPointsPerNonEmptyLeaf()));
        System.out.println(String.format("Standard Deviation: %f", quadTree.getStandardDeviationNonEmptyLeaves()));
        
        // ************* END GLOBAL INDEXING PHASE *************
        
        // ************* LOCAL INDEXING PHASE *************
        
        Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(quadTree);
        
        JavaPairRDD<Node, Point> nodePointPairs = points.mapToPair(point -> {
        	QuadTree qt = broadcastQuadTree.getValue();
        	return new Tuple2<Node, Point>(qt.determineLeafNodeForInsertion(qt.getRoot(), point), point);
        });
        
        JavaPairRDD<Node, Integer> leaves = nodePointPairs.aggregateByKey(0, (sum, point) -> sum + 1, (totalSum, sum) -> totalSum + sum);
        leaves.foreach(pair -> {
        	System.out.print(String.format("Leaf with id %d contains %d points", pair._1.getId(), pair._2));
        });
        
        // ************* END LOCAL INDEXING PHASE *************
        
        
    }
}
