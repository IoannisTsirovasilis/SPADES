package gr.ds.unipi.spades;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.ivy.ant.PackageMapping;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import gr.ds.unipi.qtree.MathUtils;
import gr.ds.unipi.qtree.Node;
import gr.ds.unipi.qtree.NodePartitioner;
import gr.ds.unipi.qtree.NodePointPair;
import gr.ds.unipi.qtree.Point;
import gr.ds.unipi.qtree.PointComparator;
import gr.ds.unipi.qtree.QuadTree;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import scala.Tuple2;

public class SpatialJoinTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public SpatialJoinTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( SpatialJoinTest.class );
    }
    
    public void testOutputsCorrectSpatialPairs() throws NumberFormatException, IOException {
    	SparkConf conf = new SparkConf().setMaster("local").setAppName("Test Spark");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class<?>[] {QuadTree.class, Node.class, Point.class, 
        	Point[].class, Node[].class, NodePointPair.class, MathUtils.class, PointComparator.class, NodePartitioner.class});
        JavaSparkContext sc = new JavaSparkContext(conf);

    	SpatialJoin sj = new SpatialJoin();
    	
    	QuadTree quadTree = new QuadTree(0, 0, 10, 10, 1);	
    	int index = 0;
    	String pathToCsv = "C:/Users/Ioannis/Desktop/qt.csv";
    	JavaRDD<String> file = sc.textFile(pathToCsv);
    	    	
    	JavaRDD<Point> points = sj.map(file);
    	
    	QuadTree qt = sj.createQuadTree(0, 0, 10, 10, 1, 1, points);
    	
    	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
    	
    	double radius = 352;
    	
    	JavaRDD<Tuple2<Point, Point>> out = sj.reduce(broadcastQuadTree, points, radius);
    	for ( Tuple2<Point, Point> pp : out.collect()) {
    		System.out.println(pp._1.toString() + " - " + pp._2.toString()); 
    	}
    }
}
