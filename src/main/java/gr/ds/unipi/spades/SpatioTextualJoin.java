package gr.ds.unipi.spades;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import gr.ds.unipi.spades.geometry.DataObject;
import gr.ds.unipi.spades.geometry.FeatureObject;
import gr.ds.unipi.spades.geometry.Point;
import gr.ds.unipi.spades.quadTree.QuadTree;
import gr.ds.unipi.spades.regularGrid.RegularGrid;
import gr.ds.unipi.spades.util.MathUtils;
import gr.ds.unipi.spades.util.Points;
import scala.Tuple2;

public class SpatioTextualJoin {
	public JavaRDD<Tuple2<Point, Point>> resultPairs;
	
	private int jaccardCount;
	private int haversineCount;
	private int pairsCount;
	
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
	
	public int getJaccardCount() {
		return jaccardCount;
	}
	
	public int getHaversineCount() {
		return haversineCount;
	}
	
	public int getPairsCount() {
		return pairsCount;
	}
	
	public void resetCounts() {
		jaccardCount = 0;
		haversineCount = 0;
		pairsCount = 0;
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
	
	public RegularGrid createRegularGrid(double minX, double minY, double maxX, double maxY, int hSectors, int vSectors) {		
		return new RegularGrid(minX, minY, maxX, maxY, hSectors, vSectors);
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
        		return new DataObject(longitude, latitude, stj.file1Tag);
        	} else if (tag == stj.file2Tag) {
        		longitude = Double.parseDouble(stj.extractWord(line, stj.file2LonIndex, stj.separator));
        		latitude = Double.parseDouble(stj.extractWord(line, stj.file2LatIndex, stj.separator));
        		keywords = stj.extractWord(line, stj.file2KeywordsIndex, stj.separator).split(stj.file2KeywordsSeparator);
        		return new FeatureObject(longitude, latitude, stj.file2Tag, keywords);
        	} else {
        		throw new IllegalArgumentException();
        	}    	
        });
        
        return points;
	}
	
	// Map to pairs
	public JavaPairRDD<Integer, Iterable<Point>> map(JavaRDD<Point> points, Broadcast<? extends Object> broadcastSpatialIndex, double radius) {
		Object spatialIndex = broadcastSpatialIndex.getValue();
		if (spatialIndex.getClass() == QuadTree.class) {
			return points.flatMapToPair(point -> {
	        	// Get broadcasted values 
	        	QuadTree qt = (QuadTree) broadcastSpatialIndex.getValue();
	        	
	        	if (point.getClass() == DataObject.class) {
	        		DataObject dataObject = (DataObject) point;
	        		return qt.assignToLeafNodeIterator(qt.getRoot(), dataObject).iterator();
	        	} else {
	        		FeatureObject featureObject = (FeatureObject) point;
	        		// else construct square around point with size length "radius" and center "point"   
	            	// 0, 90, 180, 270 represents navigation bearing
	            	double squareUpperY = MathUtils.getPointInDistanceAndBearing(featureObject, radius, 0).getY();
	            	double squareLowerY = MathUtils.getPointInDistanceAndBearing(featureObject, radius, 180).getY();
	            	double squareUpperX = MathUtils.getPointInDistanceAndBearing(featureObject, radius, 90).getX();
	            	double squareLowerX = MathUtils.getPointInDistanceAndBearing(featureObject, radius, 270).getX();
	            	
	            	featureObject.setSquare(squareLowerX, squareLowerY, squareUpperX, squareUpperY);
	            	
	            	// Assign point to every leaf that intersects with the square
	            	return qt.assignToLeafNodeAndDuplicate(qt.getRoot(), featureObject).iterator();
	        	}       	
	        }).groupByKey().mapValues(iter -> {
	        	List<Point> pp = new ArrayList<Point>((Collection<? extends Point>) iter);
	        	pp.sort(new DataObject());
	        	return pp;
	        }); // group by leaf id and sort values based on tag
		} else if (spatialIndex.getClass() == RegularGrid.class) {
			return points.flatMapToPair(point -> {
	        	// Get broadcasted values 
	        	RegularGrid grid = (RegularGrid) broadcastSpatialIndex.getValue();
	        	return point.getClass() == DataObject.class ? grid.assignToCellIterator(point).iterator() : 
	        		grid.assignToCellAndDuplicate(point, radius).iterator(); 	
	        }).groupByKey().mapValues(iter -> {
	        	List<Point> pp = new ArrayList<Point>((Collection<? extends Point>) iter);
	        	pp.sort(new DataObject());
	        	return pp;
	        });	// group by leaf id and sort values based on tag
		} else {
			throw new IllegalArgumentException("Invalid spatial index provided.");
		}
		
	}
	
	// Map to pairs
		public JavaPairRDD<Integer, Iterable<Point>> mapPlaneSweep(JavaRDD<Point> points, Broadcast<? extends Object> broadcastSpatialIndex, double radius) {
			Object spatialIndex = broadcastSpatialIndex.getValue();
			if (spatialIndex.getClass() == QuadTree.class) {
				return points.flatMapToPair(point -> {
		        	// Get broadcasted values 
		        	QuadTree qt = (QuadTree) broadcastSpatialIndex.getValue();
		        	
		        	if (point.getClass() == DataObject.class) {
		        		DataObject dataObject = (DataObject) point;
		        		return qt.assignToLeafNodeIterator(qt.getRoot(), dataObject).iterator();
		        	} else {
		        		FeatureObject featureObject = (FeatureObject) point;
		        		// else construct square around point with size length "radius" and center "point"   
		            	// 0, 90, 180, 270 represents navigation bearing
		            	double squareUpperY = MathUtils.getPointInDistanceAndBearing(featureObject, radius, 0).getY();
		            	double squareLowerY = MathUtils.getPointInDistanceAndBearing(featureObject, radius, 180).getY();
		            	double squareUpperX = MathUtils.getPointInDistanceAndBearing(featureObject, radius, 90).getX();
		            	double squareLowerX = MathUtils.getPointInDistanceAndBearing(featureObject, radius, 270).getX();
		            	
		            	featureObject.setSquare(squareLowerX, squareLowerY, squareUpperX, squareUpperY);
		            	
		            	// Assign point to every leaf that intersects with the square
		            	return qt.assignToLeafNodeAndDuplicate(qt.getRoot(), featureObject).iterator();
		        	}       	
		        }).groupByKey();
			} else if (spatialIndex.getClass() == RegularGrid.class) {
				return points.flatMapToPair(point -> {
		        	// Get broadcasted values 
		        	RegularGrid grid = (RegularGrid) broadcastSpatialIndex.getValue();
		        	return point.getClass() == DataObject.class ? grid.assignToCellIterator(point).iterator() : 
		        		grid.assignToCellAndDuplicate(point, radius).iterator(); 	
		        }).groupByKey();
			} else {
				throw new IllegalArgumentException("Invalid spatial index provided.");
			}
			
		}
		
	// Reduce
    // Produce result set (pairs of interest)
	public JavaRDD<Tuple2<Point, Point>> reduce(JavaPairRDD<Integer, Iterable<Point>> pairs,
			double radius, double similarityScore, String[] keywords, Broadcast<SpatioTextualJoin> broadcastStj) {        		
        resultPairs = pairs.flatMap((FlatMapFunction<Tuple2<Integer, Iterable<Point>>, Tuple2<Point, Point>>) pair -> {
        	SpatioTextualJoin stj = broadcastStj.getValue();
        	
        	// Array list to retain data objects in memory
        	ArrayList<Point> local = new ArrayList<Point>();
        	
        	// output is used to hold result point pairs 
        	ArrayList<Tuple2<Point, Point>> output = new ArrayList<Tuple2<Point, Point>>(); 
        	
        	for (Point point : pair._2) {
        		
        		// Load data objects
        		if (point.getClass() == DataObject.class) { 
        			local.add(point);
        			continue;
        		}
        		
        		for (Point p : local) {
    				FeatureObject featureObject = (FeatureObject) point;
    				// Check if it is within the provided distance AND is above the lower similarity threshold 
    				stj.haversineCount++;
        			if (MathUtils.haversineDistance(p, featureObject) <= radius) {
        				stj.jaccardCount++;
    					if (MathUtils.jaccardSimilarity(keywords, featureObject.getKeywords()) >= similarityScore) {
    						stj.pairsCount++;
							output.add(new Tuple2<Point, Point>(p, featureObject));
    					}
        			}
        		}
        	}        	
        	
        	return output.iterator();
        });
        
        return resultPairs;
	}
	
	// Reduce
    // Produce result set (pairs of interest)
	public JavaRDD<Tuple2<Point, Point>> reduceJaccardCount(JavaPairRDD<Integer, Iterable<Point>> pairs,
			double radius, double similarityScore, String[] keywords, Broadcast<SpatioTextualJoin> broadcastStj) {        		
        resultPairs = pairs.flatMap((FlatMapFunction<Tuple2<Integer, Iterable<Point>>, Tuple2<Point, Point>>) pair -> {
        	SpatioTextualJoin stj = broadcastStj.getValue();
        	
        	// Array list to retain data objects in memory
        	ArrayList<Point> local = new ArrayList<Point>();
        	
        	// output is used to hold result point pairs 
        	ArrayList<Tuple2<Point, Point>> output = new ArrayList<Tuple2<Point, Point>>(); 
        	
        	for (Point point : pair._2) {
        		
        		// Load data objects
        		if (point.getClass() == DataObject.class) { 
        			local.add(point);
        			continue;
        		}
        		
        		for (Point p : local) {
    				FeatureObject featureObject = (FeatureObject) point;
    				// Check if it is within the provided distance AND is above the lower similarity threshold 
    				stj.jaccardCount++;
        			if (MathUtils.jaccardSimilarity(keywords, featureObject.getKeywords()) >= similarityScore) {
        				stj.haversineCount++;
    					if (MathUtils.haversineDistance(p, featureObject) <= radius) {
    						stj.pairsCount++;
							output.add(new Tuple2<Point, Point>(p, featureObject));
    					}
        			}
        		}
        	}        	
        	
        	return output.iterator();
        });
        
        return resultPairs;
	}
	
	// Reduce
    // Produce result set (pairs of interest)
	public JavaRDD<Tuple2<Point, Point>> reduceWithPlaneSweep(JavaPairRDD<Integer, Iterable<Point>> pairs,
			double radius, double similarityScore, String[] keywords, Broadcast<SpatioTextualJoin> broadcastStj) { 
        resultPairs = pairs.flatMap((FlatMapFunction<Tuple2<Integer, Iterable<Point>>, Tuple2<Point, Point>>) pair -> {
        	SpatioTextualJoin stj = broadcastStj.getValue();
        	int size = 0;
        	for (Point p : pair._2) {
        		size++;
        	}
        	// output is used to hold result point pairs 
        	ArrayList<Tuple2<Point, Point>> output = new ArrayList<Tuple2<Point, Point>>(); 
        	
        	Point[] objects = new Point[size];
        	int counter = 0;
        	for (Point point : pair._2) {
        		objects[counter++] = point;
        	}      
        	Points.sort(objects, Point.XComparator);
        	int currentTag = 0;
        	Point end = new Point();
        	for (int i = 0; i < objects.length - 1; i++) {
        		currentTag = ((DataObject) objects[i]).getTag();
        		end.setX(MathUtils.getXInDistanceOnEquator(objects[i].getX(), radius) + 0.1);
        		
        		int j = i + 1;
        		while (objects[j].getX() <= end.getX()) {
        			if (((DataObject) objects[j]).getTag() != currentTag) {
        				FeatureObject fo = objects[i].getClass() == DataObject.class ? (FeatureObject) objects[j] : (FeatureObject) objects[i];
        				stj.jaccardCount++;
        				if (MathUtils.jaccardSimilarity(fo.getKeywords(), keywords) >= similarityScore) {      					
        					stj.haversineCount++;
        					if (MathUtils.haversineDistance(objects[i], objects[j]) <= radius) {
        						stj.pairsCount++;
        						output.add(new Tuple2<Point, Point>(objects[j], objects[i]));
        					}
        				}
        			}
        			
        			j++;
        			
        			if (j == objects.length) break;
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
