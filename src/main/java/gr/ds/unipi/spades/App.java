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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import gr.ds.unipi.qtree.MathUtils;
import gr.ds.unipi.qtree.Node;
import gr.ds.unipi.qtree.QuadTree;
import scala.Tuple2;
import gr.ds.unipi.qtree.Point;
import gr.ds.unipi.qtree.PointComparator;
import gr.ds.unipi.qtree.NodePartitioner;
import gr.ds.unipi.qtree.NodePointPair;


/**
 * Hello world!
 *
 */
public class App 
{
	private static final String FILE_PATH = "F:\\OneDrive\\Documents\\SPADES Project\\Working Drafts\\Files\\"; //"hdfs://localhost:9000/user/test/";
	private static final String OUTPUT_PATH = "C:/Users/Ioannis/Desktop/";
	public static JavaRDD<Tuple2<Point, Point>> out;
    public static void main( String[] args ) throws IOException
    {   	
    	// Initialize spark context
    	SparkConf conf = new SparkConf().setMaster("local").setAppName("Test Spark");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class<?>[] {QuadTree.class, Node.class, Point.class, 
        	Point[].class, Node[].class, NodePointPair.class, MathUtils.class, PointComparator.class, NodePartitioner.class});
        JavaSparkContext sc = new JavaSparkContext(conf);

    	SpatialJoin sj = new SpatialJoin();
    	String pathToCsv = FILE_PATH + "u_100k.txt," + FILE_PATH + "uw_100k.txt";
    	JavaRDD<String> file = sc.textFile(pathToCsv);
    	    	
    	JavaRDD<Point> points = sj.map(file);
    	points.persist(StorageLevel.MEMORY_ONLY());
    	double minX = 0;
    	double minY = 0;
    	double maxX = 50;
    	double maxY = 50;
    	int samplePointsPerLeaf = 100;
    	double samplePercentage = 0.01;
    	double similarityScore = 0.3;
    	
    	long startTime = System.nanoTime();
        
    	QuadTree qt = sj.createQuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf, samplePercentage, points);
    	long elapsedTime1 = System.nanoTime() - startTime;  
    	
    	Broadcast<QuadTree> broadcastQuadTree = sc.broadcast(qt);
    	double radius = 2;
    	String[] keywords = {"Coffee"};
    	String[] keywords3 = {"Coffee", "Juices", "Mexican"};
    	startTime = System.nanoTime();
    	JavaRDD<Tuple2<Point, Point>> out = sj.reduce(broadcastQuadTree, points, radius, similarityScore , keywords);
    	System.out.println(out.count());
    	long elapsedTime2 = System.nanoTime() - startTime;  
    	System.out.println("Quadtree create time " + elapsedTime1 / 1000000);
    	System.out.println("Result set execution time " + elapsedTime2 / 1000000);
    }
}
