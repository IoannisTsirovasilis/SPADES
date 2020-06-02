package gr.ds.unipi.spades;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Test Spark");
        JavaSparkContext sc = new JavaSparkContext(conf);
    }
}
