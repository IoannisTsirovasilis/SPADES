package gr.ds.unipi.spades;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import gr.ds.unipi.irregularGrid.IrregularGrid;
import gr.ds.unipi.irregularGrid.Point;

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
    	
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Test Spark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println("Reading file...");
        JavaRDD<String> input = sc.textFile(FILE_PATH + "hotels-ver1.txt");
        System.out.println("File read.");
        System.out.println(input.first());
        
    }
}
