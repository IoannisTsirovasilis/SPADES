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

public class SpatioTextualJoin extends Query {
	public JavaRDD<Tuple2<FeatureObject, FeatureObject>> resultPairs;
	private int file1Tag, file2Tag, keywordsIndex;

	private String keywordsSeparator;
	
	// Constructor
	public SpatioTextualJoin(int file1LonIndex, int file1LatIndex, int file1Tag,
			int file2LonIndex, int file2LatIndex, int file2Tag, int keywordsIndex, String keywordsSeparator,
			String separator, int tagIndex) {
		super(file1LonIndex, file1LatIndex, file2LonIndex, file2LatIndex, separator, tagIndex);
		this.file1Tag = file1Tag;
		this.file2Tag = file2Tag;
		this.keywordsIndex = keywordsIndex;
		this.keywordsSeparator = keywordsSeparator;
	}
	
	public HashMap<Integer, Integer> getBins() {
		return bins;
	}
	
	public void incrementPairsCount() {
		pairsCount++;
	}
	
	public void insertToBins(Integer key, Integer value) {
		if (bins.containsKey(key)) return;
		
		bins.put(key, value);
	}
	
	public void incrementBinsKey(Integer key) {
		if (!bins.containsKey(key)) { 
			insertToBins(key, new Integer(1));
			return;
		}
		bins.replace(key, new Integer(bins.get(key).intValue() + 1));
	}
	
	public int getMbrCount() {
		return mbrCount;
	}
	
	public int getQuadTreeDuplications() {
		return quadTreeDuplications;
	}
	
	public int getRegularGridDuplications() {
		return regularGridDuplications;
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
	
	public void increaseRegularGridDuplicationsBy(int n) {
		regularGridDuplications += n;
	}
	
	public void increaseQuadTreeDuplicationsBy(int n) {
		quadTreeDuplications += n;
	}
	
	public void resetCounts() {
		jaccardCount = 0;
		haversineCount = 0;
		pairsCount = 0;
		quadTreeDuplications = 0;
		regularGridDuplications = 0;
		mbrCount = 0;
		bins = new HashMap<Integer, Integer>();
	}
	
	// Create Global Index Quad Tree
	public QuadTree createQuadTree(double minX, double minY, double maxX, double maxY, 
			int samplePointsPerLeaf, int sampleSize, JavaRDD<Point> points) {
		
		// If a point is on the edges of the root's boundaries, it will throw an error. Adding a small padding
		double epsilon = 0.00001;
		
		QuadTree quadTree = new QuadTree(minX - epsilon, minY - epsilon, maxX + epsilon, maxY + epsilon, samplePointsPerLeaf);
		
		// sampling
		List<Point> sample = points.takeSample(false, sampleSize);
        
        for (Point p : sample) {
        	quadTree.insertPoint(p);
        }   
        
        return quadTree;
	}
	
	// Create Global Index Quad Tree
		public QuadTree createQuadTreeLPT(double minX, double minY, double maxX, double maxY, 
				int samplePointsPerLeaf, int sampleSize, JavaRDD<Point> points) {
			
			// If a point is on the edges of the root's boundaries, it will throw an error. Adding a small padding
			double epsilon = 0.00001;
			
			QuadTree quadTree = new QuadTree(minX - epsilon, minY - epsilon, maxX + epsilon, maxY + epsilon, samplePointsPerLeaf);
			
			// sampling
	        List<Point> sample = points.takeSample(false, sampleSize);
	        
	        for (Point p : sample) {
	        	Node node = quadTree.insertPointGetNode(p);
	        	incrementBinsKey(node.getId());
	        }   
	        
	        return quadTree;
		}
	
	public QuadTree createQuadTree(double minX, double minY, double maxX, double maxY, int samplePointsPerLeaf, int sampleSize, JavaRDD<Point> points
			, double radius) {
		
		// If a point is on the edges of the root's boundaries, it will throw an error. Adding a small padding
		double epsilon = 0.00001;
		
		QuadTree quadTree = new QuadTree(minX - epsilon, minY - epsilon, maxX + epsilon, maxY + epsilon, samplePointsPerLeaf);
		
		// sampling
        List<Point> sample = points.takeSample(false, sampleSize);
        
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
	public JavaRDD<Point> mapToPoints(JavaRDD<String> lines, Broadcast<SpatioTextualJoin> broadcastStj) {
        JavaRDD<Point> points = lines.map(line -> {
        	SpatioTextualJoin stj = broadcastStj.getValue();
        	double longitude, latitude;
        	int tag = Integer.parseInt(stj.extractWord(line, stj.tagIndex, stj.separator));
        	
        	if (tag == stj.file1Tag || tag == stj.file2Tag) {
        		longitude = Double.parseDouble(stj.extractWord(line, stj.file1LonIndex, stj.separator));
        		latitude = Double.parseDouble(stj.extractWord(line, stj.file1LatIndex, stj.separator));
        		String[] objectKeywords = stj.extractWord(line, stj.keywordsIndex, stj.separator).split(stj.keywordsSeparator);
        		return new FeatureObject(longitude, latitude, tag, objectKeywords, 0);
        	} else {
        		throw new IllegalArgumentException();
        	}    	
        });
        
        return points.filter(point -> point != null);
	}
	
	// Map to pairs
	public JavaPairRDD<Integer, Point> map(JavaRDD<Point> points, Broadcast<? extends Object> broadcastSpatialIndex, 
			double radius) {		
		Object spatialIndex = broadcastSpatialIndex.getValue();
		
		JavaPairRDD<Integer, Point> pairs;
		
		if (spatialIndex.getClass() == QuadTree.class) {
			pairs = assignPointsToNodes(points, broadcastSpatialIndex, radius);
		} else if (spatialIndex.getClass() == RegularGrid.class) {
			pairs = assignPointsToCells(points, broadcastSpatialIndex, radius);
		} else {
			throw new IllegalArgumentException("Invalid spatial index provided.");
		}
		
		return pairs;
	}
	
	// Reduce
    // Produce result set (pairs of interest)
	public JavaRDD<Tuple2<FeatureObject, FeatureObject>> reduce(JavaPairRDD<Integer, List<Point>> pairs, double radius) {        		
        resultPairs = pairs.flatMap((FlatMapFunction<Tuple2<Integer, List<Point>>, Tuple2<FeatureObject, FeatureObject>>) pair -> {
        	
        	// output is used to hold result point pairs 
        	ArrayList<Tuple2<FeatureObject, FeatureObject>> output = new ArrayList<Tuple2<FeatureObject, FeatureObject>>();
        	
        	// Array list to retain data objects in memory
        	ArrayList<FeatureObject> local = new ArrayList<FeatureObject>();       	
        	
        	for (Point point : pair._2) {
        		FeatureObject fo = (FeatureObject) point;
        		
        		// Load objects of "Left" dataset
        		if (fo.getTag() == 1) { 
        			local.add(fo);
        			continue;
        		}
        		
        		for (FeatureObject p : local) {			
    				if (MathUtils.jaccardSimilarity(fo.getKeywords(), p.getKeywords()) > 0) {
    					// Check if it is within the provided distance
            			if (MathUtils.haversineDistance(p, fo) <= radius) {
    						output.add(new Tuple2<FeatureObject, FeatureObject>(p, fo));
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
	public JavaRDD<Tuple2<FeatureObject, FeatureObject>> reduceQuadTreeMBRCheck(JavaPairRDD<Integer, List<Point>> pairs,
			double radius) {       
		resultPairs = pairs.flatMap((FlatMapFunction<Tuple2<Integer, List<Point>>, Tuple2<FeatureObject, FeatureObject>>) pair -> {
		        	
		        	// output is used to hold result point pairs 
		        	ArrayList<Tuple2<FeatureObject, FeatureObject>> output = new ArrayList<Tuple2<FeatureObject, FeatureObject>>();
		        	
		        	// Array list to retain data objects in memory
		        	ArrayList<FeatureObject> local = new ArrayList<FeatureObject>();       	
		        	
		        	for (Point point : pair._2) {
		        		FeatureObject fo = (FeatureObject) point;
		        		
		        		// Load objects of "Left" dataset
		        		if (fo.getTag() == 1) { 
		        			local.add(fo);
		        			continue;
		        		}
		        		
		        		for (FeatureObject p : local) {			
		    				if (MathUtils.jaccardSimilarity(fo.getKeywords(), p.getKeywords()) > 0) {
		    					// Check if it is within the provided distance
		    					if (MathUtils.rectangleContains(p.getSquareLowerX(), p.getSquareLowerY(), 
		        						p.getSquareUpperX(), p.getSquareUpperY(), fo)) {
			            			if (MathUtils.haversineDistance(p, fo) <= radius) {
			    						output.add(new Tuple2<FeatureObject, FeatureObject>(p, fo));
			            			}
		    					}
		    				}
		    				
		        		}
		        	}        	
		        	
		        	return output.iterator();
		        });
		        
	        return resultPairs;
	}
	
	private JavaPairRDD<Integer, Point> assignPointsToNodes(JavaRDD<Point> points, Broadcast<? extends Object> broadcastSpatialIndex, 
			double radius) {
    	return points.flatMapToPair(point -> {
        	// Get broadcasted values 
        	QuadTree qt = (QuadTree) broadcastSpatialIndex.getValue();
        	FeatureObject featureObject = (FeatureObject) point;
        	if (featureObject.getTag() == 1) {
        		ArrayList<Tuple2<Integer, Point>> result = qt.assignToLeafNodeIterator(qt.getRoot(), featureObject);
        		return result.iterator();
        	} else {

				// else construct square around point with size length "radius" and center "point"   
            	// 0, 90, 180, 270 represents navigation bearing
            	double squareUpperY = MathUtils.getPointInDistanceAndBearing(featureObject, radius, 0).getY();
            	double squareLowerY = MathUtils.getPointInDistanceAndBearing(featureObject, radius, 180).getY();
            	double squareUpperX = MathUtils.getPointInDistanceAndBearing(featureObject, radius, 90).getX();
            	double squareLowerX = MathUtils.getPointInDistanceAndBearing(featureObject, radius, 270).getX();
            	
            	featureObject.setSquare(squareLowerX, squareLowerY, squareUpperX, squareUpperY);
            	
            	// Assign point to every leaf that intersects with the square
            	ArrayList<Tuple2<Integer, Point>> result = qt.assignToLeafNodeAndDuplicate(qt.getRoot(), featureObject);

            	return result.iterator();
        	}
        });	
	}
	
	private JavaPairRDD<Integer, Point> assignPointsToCells(JavaRDD<Point> points, Broadcast<? extends Object> broadcastSpatialIndex, 
			double radius) {
		return points.flatMapToPair(point -> {
        	// Get broadcasted values 
        	RegularGrid grid = (RegularGrid) broadcastSpatialIndex.getValue();
        	FeatureObject featureObject = (FeatureObject) point;
        	if (featureObject.getTag() == 1) {
        		ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellIterator(point);
        		return result.iterator();
        	} else {
        		ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
            	return result.iterator();
        	}            	
        });
    }
}