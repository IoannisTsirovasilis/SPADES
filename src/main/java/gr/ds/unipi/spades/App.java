package gr.ds.unipi.spades;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
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
import gr.ds.unipi.qtree.NodePointPair;


/**
 * Hello world!
 *
 */
public class App 
{
	private static final String FILE_PATH = "F:\\OneDrive\\Documents\\SPADES Project\\Working Drafts\\Files\\"; //"hdfs://localhost:9000/user/test/";
	private static final String OUTPUT_PATH = "C:/Users/Ioannis/Desktop/";
    public static void main( String[] args ) throws IOException
    {   	
    	// Initialize spark context
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Test Spark");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class<?>[] {QuadTree.class, Node.class, Point.class, 
        	Point[].class, Node[].class, NodePointPair.class, MathUtils.class, FileWriter.class});
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // Constant parameters
        int file1lonIndex = 5;
    	int file1latIndex = 4;
    	String file1separator = "|";
    	
    	int file2lonIndex = 4;
    	int file2latIndex = 3;
    	String file2separator = "|";
    	
    	double samplePecentage = 0.01;
    	double epsilon = 0.000001;
    	int pointsPerLeaf = 1_000;
    	int samplePointsPerLeaf = (int) (pointsPerLeaf * samplePecentage);
    	String file1Name = "hotels.txt";
    	String file2Name = "restaurants.txt";
    	
    	double minX = -130, minY = -60, maxX = 60, maxY = 80;
    	
    	// query radius (kilometers)
    	double radius = 2;
        
        // Read sample file
        System.out.println("Reading file...");
        JavaRDD<String> file1 = sc.textFile(FILE_PATH + file1Name + "," + FILE_PATH + file2Name);
        //JavaRDD<String> file2 = sc.textFile(FILE_PATH + file2Name);
        System.out.println("File read.");
        
        // Create (empty) quad tree at MASTER node
        QuadTree quadTree = new QuadTree(minX - epsilon, minY - epsilon, maxX + epsilon, maxY + epsilon, samplePointsPerLeaf);
        
        // Map
        // Extract point information
        JavaRDD<Point> points1 = file1.map(line -> {
        	String lon, lat;
        	int lonOrdinalIndex, latOrdinalIndex;
        	int filelonIndex, filelatIndex;
        	int fileTag;
        	String fileseparator;
        	
        	// Hotels
        	if (StringUtils.countMatches(line, '|') == 6) {
        		filelonIndex = 5;
        		filelatIndex = 4;
        		fileseparator = "|";
        		fileTag = 1;
        	// Restaurants
        	} else {
        		filelonIndex = 4;
        		filelatIndex = 3;
        		fileseparator = "|";
        		fileTag = 2;
        	}
        	
        	if (filelonIndex == 0) {
    			lon = line.substring(0, line.indexOf(fileseparator));
    		} else {
    			lonOrdinalIndex = StringUtils.ordinalIndexOf(line, file1separator, filelonIndex);
    			
    			if (lonOrdinalIndex == StringUtils.lastOrdinalIndexOf(line, fileseparator, 1))
        		{
        			lon =  line.substring(lonOrdinalIndex + 1);
        		} else {
        			lon = line.substring(lonOrdinalIndex + 1, StringUtils.ordinalIndexOf(line, fileseparator, filelonIndex + 1));
        		}    			
    		}
    		
    		
        	if (filelatIndex == 0) {
    			lat = line.substring(0, line.indexOf(fileseparator));
    		} else {
    			latOrdinalIndex = StringUtils.ordinalIndexOf(line, fileseparator, filelatIndex);
    			
    			if (latOrdinalIndex == StringUtils.lastOrdinalIndexOf(line, fileseparator, 1))
        		{
        			lat =  line.substring(latOrdinalIndex + 1);
        		} else {
        			lat = line.substring(latOrdinalIndex + 1, StringUtils.ordinalIndexOf(line, fileseparator, filelatIndex + 1));
        		}
    		}
        	
    		return new Point(Double.parseDouble(lon), Double.parseDouble(lat), fileTag);         	
        });
        
//        JavaRDD<Point> points2 = file2.map(line -> {
//        	String lon, lat;
//        	int lonOrdinalIndex, latOrdinalIndex;
//        	if (file2lonIndex == 0) {
//    			lon = line.substring(0, line.indexOf(file2separator));
//    		} else {
//    			lonOrdinalIndex = StringUtils.ordinalIndexOf(line, file2separator, file2lonIndex);
//    			
//    			if (lonOrdinalIndex == StringUtils.lastOrdinalIndexOf(line, file2separator, 1))
//        		{
//        			lon =  line.substring(lonOrdinalIndex + 1);
//        		} else {
//        			lon = line.substring(lonOrdinalIndex + 1, StringUtils.ordinalIndexOf(line, file2separator, file2lonIndex + 1));
//        		}    			
//    		}
//    		
//    		
//        	if (file2latIndex == 0) {
//    			lat = line.substring(0, line.indexOf(file2separator));
//    		} else {
//    			latOrdinalIndex = StringUtils.ordinalIndexOf(line, file2separator, file2latIndex);
//    			
//    			if (latOrdinalIndex == StringUtils.lastOrdinalIndexOf(line, file2separator, 1))
//        		{
//        			lat =  line.substring(latOrdinalIndex + 1);
//        		} else {
//        			lat = line.substring(latOrdinalIndex + 1, StringUtils.ordinalIndexOf(line, file2separator, file2latIndex + 1));
//        		}
//    		}
//        	
//    		return new Point(Double.parseDouble(lon), Double.parseDouble(lat), 2);         	
//        });
        
        
        // ************* GLOBAL INDEXING PHASE *************        
        // Insert a sample of the data set (1% of total size) into the quad tree
        
        // sampling
        List<Point> sample1 = points1.takeSample(false, (int) (points1.count() * samplePecentage));
        //List<Point> sample2 = points2.takeSample(false, (int) (points2.count() * samplePecentage));
        
        long startTime = System.nanoTime();
        for (Point p : sample1) {
        	quadTree.insertPoint(p);
        }   
        
//        for (Point p : sample2) {
//        	quadTree.insertPoint(p);
//        }  
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
        
        JavaPairRDD<Node, Point> nodePointPairs1 = points1.flatMapToPair(point -> {
        	QuadTree qt = broadcastQuadTree.getValue();
        	if (point.getTag() == 1)
        		return qt.assignToLeafNodeIterator(qt.getRoot(), point).iterator();
        	
        	double mbrUpperY = MathUtils.getPointInDistanceAndBearing(point, radius, 0).getY();
        	double mbrLowerY = MathUtils.getPointInDistanceAndBearing(point, radius, 180).getY();
        	double mbrUpperX = MathUtils.getPointInDistanceAndBearing(point, radius, 90).getX();
        	double mbrLowerX = MathUtils.getPointInDistanceAndBearing(point, radius, 270).getX();
        	point.setMBR(mbrLowerX, mbrLowerY, mbrUpperX, mbrUpperY);
        	return qt.assignToLeafNodeAndDuplicate(qt.getRoot(), point).iterator();
        });
        
//        JavaPairRDD<Node, Point> nodePointPairs2 = points2.flatMapToPair(point -> {
//        	QuadTree qt = broadcastQuadTree.getValue();
//        	double mbrUpperY = MathUtils.getPointInDistanceAndBearing(point, radius, 0).getY();
//        	double mbrLowerY = MathUtils.getPointInDistanceAndBearing(point, radius, 180).getY();
//        	double mbrUpperX = MathUtils.getPointInDistanceAndBearing(point, radius, 90).getX();
//        	double mbrLowerX = MathUtils.getPointInDistanceAndBearing(point, radius, 270).getX();
//        	point.setMBR(mbrLowerX, mbrLowerY, mbrUpperX, mbrUpperY);
//        	return qt.assignToLeafNodeAndDuplicate(qt.getRoot(), point).iterator();
//        });
//        
        
        
        JavaPairRDD<Node, Integer> leaves = nodePointPairs1.aggregateByKey(0, (sum, point) -> sum + 1, (totalSum, sum) -> totalSum + sum);
        leaves.foreach(pair -> {
        	System.out.println(String.format("Leaf with id %d contains %d points", pair._1.getId(), pair._2));
        });
        
        // ************* END LOCAL INDEXING PHASE *************
        
        
        
        JavaPairRDD<Node, Iterable<Point>> groupByNode = nodePointPairs1.groupByKey();
        groupByNode.saveAsTextFile(OUTPUT_PATH + "New");
//        groupByNode.foreach(pair -> {        
//        	ArrayList<Point> points = new ArrayList<Point>();
//        	for (Point point : pair._2) {
//        		if (point.getTag() == 1) {
//        			points.add(point);
//        			continue;
//        		}
//        		
//        		for (Point p : points) {
//        			if (MathUtils.haversineDistance(p, point) <= radius) {
//        				
//            		}
//        		}
//        		
//        	}
//        });
    }
}
