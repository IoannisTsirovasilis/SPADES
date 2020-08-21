package gr.ds.unipi.spades.queries;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import gr.ds.unipi.spades.geometry.DataObject;
import gr.ds.unipi.spades.geometry.FeatureObject;
import gr.ds.unipi.spades.geometry.Point;
import gr.ds.unipi.spades.quadTree.Node;
import gr.ds.unipi.spades.quadTree.QuadTree;
import gr.ds.unipi.spades.regularGrid.RegularGrid;
import gr.ds.unipi.spades.util.MathUtils;
import scala.Tuple2;

public class TopK extends Query {
	public JavaRDD<Tuple2<Point, Point>> resultPairs;
	private int file1Tag, file2Tag, file2KeywordsIndex;

	private String file2KeywordsSeparator;
	
	// Constructor
	public TopK(int file1LonIndex, int file1LatIndex, int file1Tag,
			int file2LonIndex, int file2LatIndex, int file2Tag, int file2KeywordsIndex, String file2KeywordsSeparator,
			String separator, int tagIndex) {
		super(file1LonIndex, file1LatIndex, file2LonIndex, file2LatIndex, separator, tagIndex);
		this.file1Tag = file1Tag;
		this.file2Tag = file2Tag;
		this.file2KeywordsIndex = file2KeywordsIndex;
		this.file2KeywordsSeparator = file2KeywordsSeparator;
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
	
	// Create Global Index Quad Tree
		public QuadTree createQuadTreeLPT(double minX, double minY, double maxX, double maxY, int samplePointsPerLeaf, double samplePercentage, JavaRDD<Point> points) {
			
			// If a point is on the edges of the root's boundaries, it will throw an error. Adding a small padding
			double epsilon = 0.00001;
			
			QuadTree quadTree = new QuadTree(minX - epsilon, minY - epsilon, maxX + epsilon, maxY + epsilon, samplePointsPerLeaf);
			
			// sampling
	        List<Point> sample = points.takeSample(false, (int) (points.count() * samplePercentage));
	        
	        for (Point p : sample) {
	        	Node node = quadTree.insertPointGetNode(p);
	        	incrementBinsKey(node.getId());
	        }   
	        
	        return quadTree;
		}
	
	public QuadTree createQuadTree(double minX, double minY, double maxX, double maxY, int samplePointsPerLeaf, double samplePercentage, JavaRDD<Point> points
			, double radius) {
		
		// If a point is on the edges of the root's boundaries, it will throw an error. Adding a small padding
		double epsilon = 0.00001;
		
		QuadTree quadTree = new QuadTree(minX - epsilon, minY - epsilon, maxX + epsilon, maxY + epsilon, samplePointsPerLeaf);
		
		// sampling
        List<Point> sample = points.takeSample(false, (int) (points.count() * samplePercentage));
        
        for (Point p : sample) {
        	quadTree.insertPoint(p, radius);
        }   
        
        return quadTree;
	}
	
	public RegularGrid createRegularGrid(double minX, double minY, double maxX, double maxY, int hSectors, int vSectors) {		
		return new RegularGrid(minX, minY, maxX, maxY, hSectors, vSectors);
	}
	
	// Map
    // Extract point information
	public JavaRDD<Point> mapToPoints(JavaRDD<String> lines, Broadcast<TopK> broadcastTopK, String[] keywords) {
        JavaRDD<Point> points = lines.map(line -> {
        	TopK topK = broadcastTopK.getValue();
        	double longitude, latitude;
        	String[] objectKeywords = null;
        	double textualRelevanceScore;
        	int tag = Integer.parseInt(topK.extractWord(line, topK.tagIndex, topK.separator));
        	
        	if (tag == topK.file1Tag) {
        		longitude = Double.parseDouble(topK.extractWord(line, topK.file1LonIndex, topK.separator));
        		latitude = Double.parseDouble(topK.extractWord(line, topK.file1LatIndex, topK.separator));
        		return new DataObject(longitude, latitude, topK.file1Tag);
        	} else if (tag == topK.file2Tag) {
        		longitude = Double.parseDouble(topK.extractWord(line, topK.file2LonIndex, topK.separator));
        		latitude = Double.parseDouble(topK.extractWord(line, topK.file2LatIndex, topK.separator));
        		objectKeywords = topK.extractWord(line, topK.file2KeywordsIndex, topK.separator).split(topK.file2KeywordsSeparator);
        		textualRelevanceScore = MathUtils.jaccardSimilarity(objectKeywords, keywords);
        		if (textualRelevanceScore == 0) return null;
        		return new FeatureObject(longitude, latitude, topK.file2Tag, objectKeywords, textualRelevanceScore);
        	} else {
        		throw new IllegalArgumentException();
        	}    	
        });
        
        return points.filter(point -> point != null);
	}
	
	// Map to pairs
	public JavaPairRDD<Integer, List<Point>> map(JavaRDD<Point> points, Broadcast<? extends Object> broadcastSpatialIndex, 
			double radius) {		
		Object spatialIndex = broadcastSpatialIndex.getValue();
		
		JavaPairRDD<Integer, Point> pairs;
		
		if (spatialIndex.getClass() == QuadTree.class) {
			pairs = QuadTree.assignPointsToNodes(points, broadcastSpatialIndex, radius);
		} else if (spatialIndex.getClass() == RegularGrid.class) {
			pairs = RegularGrid.assignPointsToCells(points, broadcastSpatialIndex, radius);
		} else {
			throw new IllegalArgumentException("Invalid spatial index provided.");
		}
		
		return pairs.groupByKey().mapValues(iter -> {
        	List<Point> pp = new ArrayList<Point>((Collection<? extends Point>) iter);
        	pp.sort(FeatureObject.Comparator);
        	return pp;
        });	// group by leaf id and sort values based on tag
	}
	
	// Reduce
    // Produce result set (pairs of interest)
	public JavaRDD<Tuple2<Point, Point>> reduce(JavaPairRDD<Integer, List<Point>> pairs,
			double radius, int numberOfResults) {        		
        resultPairs = pairs.flatMap((FlatMapFunction<Tuple2<Integer, List<Point>>, Tuple2<Point, Point>>) pair -> {
        	
        	// output is used to hold result point pairs 
        	ArrayList<Tuple2<Point, Point>> output = new ArrayList<Tuple2<Point, Point>>();
        	
        	// Array list to retain data objects in memory
        	ArrayList<Point> local = new ArrayList<Point>();       	
        	
        	int counter = 0;
        	for (Point point : pair._2) {
        		// Load data objects
        		if (point.getClass() == DataObject.class) { 
        			local.add(point);
        			continue;
        		}
        		
        		for (Point p : local) {
    				FeatureObject featureObject = (FeatureObject) point;
    				// Check if it is within the provided distance
        			if (MathUtils.haversineDistance(p, featureObject) <= radius) {
						output.add(new Tuple2<Point, Point>(p, featureObject));
						counter++;
						
						if (counter == numberOfResults) return output.iterator();
        			}
        		}
        	}        	
        	
        	return output.iterator();
        });
        
        return resultPairs;
	}
	
	// Reduce
    // Produce result set (pairs of interest)
	public JavaRDD<Tuple2<Point, Point>> reduceQuadTreeMBRCheck(JavaPairRDD<Integer, List<Point>> pairs,
			double radius, int numberOfResults) {        		
        resultPairs = pairs.flatMap((FlatMapFunction<Tuple2<Integer, List<Point>>, Tuple2<Point, Point>>) pair -> {
        	
        	// output is used to hold result point pairs 
        	ArrayList<Tuple2<Point, Point>> output = new ArrayList<Tuple2<Point, Point>>();
        	
        	// Array list to retain data objects in memory
        	ArrayList<Point> local = new ArrayList<Point>();       	
        	
        	int counter = 0;
        	for (Point point : pair._2) {
        		// Load data objects
        		if (point.getClass() == DataObject.class) { 
        			local.add(point);
        			continue;
        		}
        		
        		for (Point p : local) {
    				FeatureObject featureObject = (FeatureObject) point;
    				// Check if it is within the provided distance
    				if (MathUtils.rectangleContains(featureObject.getSquareLowerX(), featureObject.getSquareLowerY(), 
    						featureObject.getSquareUpperX(), featureObject.getSquareUpperY(), point)) {
    					if (MathUtils.haversineDistance(p, featureObject) <= radius) {
    						output.add(new Tuple2<Point, Point>(p, featureObject));
    						counter++;
    						
    						if (counter == numberOfResults) return output.iterator();
            			}
    				}        			
        		}
        	}        	
        	
        	return output.iterator();
        });
        
        return resultPairs;
	}
}
