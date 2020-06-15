package gr.ds.unipi.spades;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import gr.ds.unipi.qtree.MathUtils;
import gr.ds.unipi.qtree.Point;
import gr.ds.unipi.qtree.QuadTree;
import scala.Tuple2;

public class SpatioTextualJoin {
	public JavaRDD<Tuple2<Point, Point>> resultPairs;
	// File 1 Fields (Data Objects)
	private int file1LonIndex, file1LatIndex, file1Tag;
	
	// File 2 Fields (Feature Objects)
	private int file2LonIndex, file2LatIndex, file2Tag, file2KeywordsIndex; 
	
	// This MUST be a Regex
	private String file2KeywordsSeparator;
	
	// File separator (must be the same for both files)
	private String separator; 
	
	// Index of tag in each record of a file (must be the same for both files)
	private int tagIndex;
	
	public SpatioTextualJoin() {}
	
	// Constructor
	public SpatioTextualJoin(int file1LonIndex, int file1LatIndex, int file1Tag,
			int file2LonIndex, int file2LatIndex, int file2Tag, int file2KeywordsIndex, String file2KeywordsSeparator,
			String separator, int tagIndex) {
		this.file1LonIndex = file1LonIndex;
		this.file1LatIndex = file1LatIndex;
		this.file1Tag = file1Tag;
		this.file2LonIndex = file2LonIndex;
		this.file2LatIndex = file2LatIndex;
		this.file2Tag = file2Tag;
		this.file2KeywordsIndex = file2KeywordsIndex;
		this.file2KeywordsSeparator = file2KeywordsSeparator;
		this.separator = separator;
		this.tagIndex = tagIndex;
	}
	
	// Create Global Index Quad Tree
	public QuadTree createQuadTree(double minX, double minY, double maxX, double maxY, int samplePointsPerLeaf, double samplePercentage, JavaRDD<Point> points) {
		
		// If a point is on the edges of the root's boundaries, it will throw an error. Adding a small padding
		double epsilon = 0.00001;
		
		QuadTree quadTree = new QuadTree(minX - epsilon, minY - epsilon, maxX + epsilon, maxY + epsilon, samplePointsPerLeaf);
		
		// sampling
        List<Point> sample = points.takeSample(false, (int) (points.count() * samplePercentage));
        
        for (Point p : sample) {
        	quadTree.insertPoint(p);
        }   
        
        return quadTree;
	}
	
	// Map
    // Extract point information
	public JavaRDD<Point> mapToPoints(JavaRDD<String> lines, Broadcast<SpatioTextualJoin> broadcastStj) {
        JavaRDD<Point> points = lines.map(line -> {
        	SpatioTextualJoin stj = broadcastStj.getValue();
        	Double longitude, latitude;
        	String[] keywords = null;
        	
        	int tag = Integer.parseInt(stj.extractWord(line, stj.tagIndex, stj.separator));
        	
        	if (tag == stj.file1Tag) {
        		longitude = Double.parseDouble(stj.extractWord(line, stj.file1LonIndex, stj.separator));
        		latitude = Double.parseDouble(stj.extractWord(line, stj.file1LatIndex, stj.separator));
        	} else if (tag == stj.file2Tag) {
        		longitude = Double.parseDouble(stj.extractWord(line, stj.file2LonIndex, stj.separator));
        		latitude = Double.parseDouble(stj.extractWord(line, stj.file2LatIndex, stj.separator));
        		keywords = stj.extractWord(line, stj.file2KeywordsIndex, stj.separator).split(stj.file2KeywordsSeparator);
        	} else {
        		throw new IllegalArgumentException();
        	}
        	Point p = new Point(longitude, latitude, tag, keywords);
    		return p;         	
        });
        
        return points;
	}
	
	// Map to pairs
	public JavaPairRDD<Integer, Iterable<Point>> map(JavaRDD<Point> points, Broadcast<QuadTree> broadcastQuadTree, 
			Broadcast<SpatioTextualJoin> broadcastStj, double radius) {
		return points.flatMapToPair(point -> {
        	// Get broadcasted values 
        	QuadTree qt = broadcastQuadTree.getValue();
        	SpatioTextualJoin stj = broadcastStj.getValue();
        	
        	// If the point comes from file 1, just assign it to its leaf node
        	if (point.getTag() == stj.file1Tag)
        		return qt.assignToLeafNodeIterator(qt.getRoot(), point).iterator();
        	
        	// else construct square around point with size length "radius" and center "point"   
        	// 0, 90, 180, 270 represents navigation bearing
        	double squareUpperY = MathUtils.getPointInDistanceAndBearing(point, radius, 0).getY();
        	double squareLowerY = MathUtils.getPointInDistanceAndBearing(point, radius, 180).getY();
        	double squareUpperX = MathUtils.getPointInDistanceAndBearing(point, radius, 90).getX();
        	double squareLowerX = MathUtils.getPointInDistanceAndBearing(point, radius, 270).getX();
        	
        	point.setSquare(squareLowerX, squareLowerY, squareUpperX, squareUpperY);
        	
        	// Assign point to every leaf that intersects with the square
        	return qt.assignToLeafNodeAndDuplicate(qt.getRoot(), point).iterator();
        }).groupByKey(); // group by leaf id
	}
	
	// Reduce
    // Produce result set (pairs of interest)
	public JavaRDD<Tuple2<Point, Point>> reduce(JavaPairRDD<Integer, Iterable<Point>> pairs, Broadcast<SpatioTextualJoin> broadcastStj, 
			double radius, double similarityScore, String[] keywords) {        		
        resultPairs = pairs.flatMap((FlatMapFunction<Tuple2<Integer, Iterable<Point>>, Tuple2<Point, Point>>) pair -> {
        	SpatioTextualJoin stj = broadcastStj.getValue();
        	
        	// Array list to retain data objects in memory
        	ArrayList<Point> local = new ArrayList<Point>();
        	
        	// output is used to hold result point pairs 
        	ArrayList<Tuple2<Point, Point>> output = new ArrayList<Tuple2<Point, Point>>(); 
        	
        	// Naturally spark retains order of elements in an RDD. Data objects come first
        	for (Point point : pair._2) {
        		
        		// Load data objects
        		if (point.getTag() == stj.file1Tag) { 
        			local.add(point);
        			continue;
        		}
        		
        		// If it reaches this line, then it processes feature objects 
    			for (Point p : local) {
    				// Check if it is within the provided distance AND is above the lower similarity threshold 
        			if (MathUtils.haversineDistance(p, point) <= radius && MathUtils.jaccardSimilarity(keywords, point.getKeywords()) >= similarityScore) {
        				output.add(new Tuple2<Point, Point>(p, point));
        			}
        		}
        	}
        	
        	return output.iterator();
        });
        
        return resultPairs;
	}
	
	// Method for extracting a substring of a delimited String 
	private String extractWord(String record, int index, String separator) {
		String word;
		int ordinalIndex;
		if (index == 0) {
			word = record.substring(0, record.indexOf(separator));
		} else {
			ordinalIndex = StringUtils.ordinalIndexOf(record, separator, index);
			
			if (ordinalIndex == StringUtils.lastOrdinalIndexOf(record, separator, 1))
    		{
				word =  record.substring(ordinalIndex + 1);
    		} else {
    			word = record.substring(ordinalIndex + 1, StringUtils.ordinalIndexOf(record, separator, index + 1));
    		}    			
		}
		
		return word;
	}
}
