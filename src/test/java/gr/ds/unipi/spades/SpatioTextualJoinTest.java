package gr.ds.unipi.spades;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import gr.ds.unipi.spades.geometry.DataObject;
import gr.ds.unipi.spades.geometry.FeatureObject;
import gr.ds.unipi.spades.geometry.Point;
import gr.ds.unipi.spades.geometry.Rectangle;
import gr.ds.unipi.spades.quadTree.Node;
import gr.ds.unipi.spades.quadTree.QuadTree;
import gr.ds.unipi.spades.queries.SpatioTextualJoin;
import gr.ds.unipi.spades.util.MathUtils;
import javassist.bytecode.Descriptor.Iterator;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import scala.Tuple2;

public class SpatioTextualJoinTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public SpatioTextualJoinTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( SpatioTextualJoinTest.class );
    }
    
    public void testOutputsCorrectSpatialPairs() throws NumberFormatException, IOException {
    	SparkConf conf = new SparkConf().setMaster("local").setAppName("Test Spark");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class<?>[] {QuadTree.class, Node.class, Point.class, 
        	Point[].class, Node[].class, MathUtils.class, TopKApp.class });
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        SpatioTextualJoin stj = new SpatioTextualJoin(1, 2, 1, 1, 2, 2, 3, "\\|", ",", 0);
        String[] keywords = new String[] {"italian"};
        Broadcast<SpatioTextualJoin> broadcastStj = sc.broadcast(stj);
        String FILE_PATH = "C:\\Users\\user\\OneDrive\\Documents\\SPADES Project\\Datasets\\";
    	String pathToCsv = FILE_PATH + "dataObjects.csv," + FILE_PATH + "featureObjects.csv";
    	JavaRDD<String> file = sc.textFile(pathToCsv);
    	JavaRDD<FeatureObject> points = stj.mapToPoints(file, broadcastStj);
    	
    	QuadTree qt = stj.createQuadTree(0, 0, 10, 10, 1, 1, points);
    	
    	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
    	double radius = 400;
    	
    	JavaPairRDD<Integer, FeatureObject> pairs = stj.map(points, broadcastQuadTree, radius);            	
    	
    	// Group By Key
    	JavaPairRDD<Integer, List<FeatureObject>> groupedPairs = pairs.groupByKey().mapValues(iter -> {
        	List<FeatureObject> pp = new ArrayList<FeatureObject>((Collection<FeatureObject>) iter);
        	pp.sort(DataObject.Comparator);
        	return pp;
        });	// group by leaf id and sort values based on tag
    	
    	JavaRDD<Tuple2<FeatureObject, FeatureObject>> out = stj.reduceForTest(groupedPairs, radius);
    	System.out.println(out.count());
    	for ( Tuple2<FeatureObject, FeatureObject> pp : out.collect()) {
    		System.out.println("(" + pp._1.toString() +  " - " + pp._2.toString());
    	}
    	sc.close();
    }
    
//    public void testReduceWithPlaneSweep() throws NumberFormatException, IOException {
//    	SparkConf conf = new SparkConf().setMaster("local").setAppName("Test Spark");
//        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        conf.registerKryoClasses(new Class<?>[] {QuadTree.class, Node.class, Point.class, 
//        	Point[].class, Node[].class, MathUtils.class, SpatioTextualJoin.class });
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        SpatioTextualJoin stj = new SpatioTextualJoin(1, 2, 1, 1, 2, 2, 3, "\\|", ",", 0);
//        String[] keywords = new String[] {"italian"};
//        Broadcast<SpatioTextualJoin> broadcastStj = sc.broadcast(stj);
//        String FILE_PATH = "C:\\Users\\user\\OneDrive\\Documents\\SPADES Project\\Datasets\\";
//    	String pathToCsv = FILE_PATH + "dataObjects.csv," + FILE_PATH + "featureObjects.csv";
//    	JavaRDD<String> file = sc.textFile(pathToCsv);
//    	JavaRDD<Point> points = stj.mapToPoints(file, broadcastStj, );
//    	
//    	QuadTree qt = stj.createQuadTree(0, 0, 10, 10, 1, 1, points);
//    	
//    	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
//    	
//    	double radius = 400;
//    	JavaPairRDD<Integer, Iterable<Point>> groupedPairs = stj.map(points, broadcastQuadTree, radius, new String[] {"italian"});
//    	JavaRDD<Tuple2<Point, Point>> out = stj.reduceWithPlaneSweep(groupedPairs, radius, 0.5 , new String[] {"italian"}, broadcastStj);
//    	System.out.println(out.count());
//    	for ( Tuple2<Point, Point> pp : out.collect()) {
//    		System.out.println(pp._1.toString() + " - " + pp._2.toString()); 
//    	}
//    	sc.close();
//
//    }
}
