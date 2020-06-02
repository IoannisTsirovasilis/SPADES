package gr.ds.unipi.spades;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import gr.ds.unipi.qtree.*;

/**
 * Hello world!
 *
 */
public class App 
{
	private static final String FILE_PATH = "hdfs://localhost:9000/user/test/";
    public static void main( String[] args )
    {
    	int lonIndex = 5;
    	int latIndex = 4;
    	double epsilon = 0.000001;
    	int pointsPerLeaf = 500;
    	double minX = -124.586307, minY = -42.118331, maxX = 47.669728, maxY = 49.00106;
    	
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Test Spark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println("Reading file...");
        JavaRDD<String> rdd = sc.textFile(FILE_PATH + "hotels-ver1.txt");
        System.out.println("File read.");
        System.out.println(rdd.first());
        
        QuadTree quadTree = QuadTree.newQuadTree(minX - epsilon, minY - epsilon,
        		maxX + epsilon, maxY + epsilon, pointsPerLeaf);
        
        JavaRDD<Point> result = rdd.map(line -> {
        	String[] words = line.split("|");
    		return Point.newPoint(Double.parseDouble(words[lonIndex]), Double.parseDouble(words[latIndex]));
        });
        
        long startTime = System.nanoTime();
        result.foreachAsync(point -> quadTree.insertPoint(point));
        long elapsedTime = System.nanoTime() - startTime;        
     
        quadTree.traverse();
        
        System.out.println("Total execution time (ms): "  + elapsedTime/1000000);
        System.out.println("Number of contained points of Tree "+ quadTree.getNumberOfInsertedPoints());
        System.out.println(String.format("Number of Leaves: %d", quadTree.getNumberOfLeaves()));
        System.out.println(String.format("Number of Full Leaves: %d", quadTree.getNumberOfFullLeaves()));
        System.out.println(String.format("Number of Empty Leaves: %d", quadTree.getNumberOfEmptyLeaves()));
        System.out.println(String.format("Points per Leaf: %f", quadTree.getMeanNumberOfPointsPerLeaf()));
        System.out.println(String.format("Standard Deviation: %f", quadTree.getStandardDeviation()));
    }
}
