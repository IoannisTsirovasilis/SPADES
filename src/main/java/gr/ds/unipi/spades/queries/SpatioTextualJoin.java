package gr.ds.unipi.spades.queries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
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
	public JavaRDD<Tuple2<Integer, Integer>> resultPairs;
	public JavaRDD<Tuple2<FeatureObject, FeatureObject>> resultPairsTest;
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
	
	public double getSampleJoinSelectivity(Node node, double radius) {
		if (node.getLDatasetPoints() == 0 || node.getRDatasetPoints() == 0) {
			return -1;
		}
		
		Point[] lPoints = new Point[node.getLDatasetPoints()];
		Point[] rPoints = new Point[node.getRDatasetPoints()];
		int l = 0;
		int r = 0;
		for (int i = 0; i < node.getNumberOfContainedPoints(); i++) {
			System.out.println("l=" + l + " - r=" + r);
			if (node.getPoints().get(i).getTag() == 1) lPoints[l++] = node.getPoints().get(i);
			else rPoints[r++] = node.getPoints().get(i);
		}
		double numerator = 0;
		for (l = 0; l < lPoints.length; l++) {
			for (r = 0; r < rPoints.length; r++) {
				if (MathUtils.haversineDistance(lPoints[l], rPoints[r]) <= radius) {
					numerator++;
				}
			}
		}
		
		return numerator / (node.getLDatasetPoints() * node.getRDatasetPoints());
	}
	
	public HashMap<Integer, Integer> getBins() {
		return bins;
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
	
	// Create Global Index Quad Tree
	public QuadTree createQuadTree(double minX, double minY, double maxX, double maxY, 
			int samplePointsPerLeaf, int sampleSize, JavaRDD<FeatureObject> points) {
		
		// If a point is on the edges of the root's boundaries, it will throw an error. Adding a small padding
		double epsilon = 0.00001;
		
		QuadTree quadTree = new QuadTree(minX - epsilon, minY - epsilon, maxX + epsilon, maxY + epsilon, samplePointsPerLeaf);
		
		// sampling
		List<FeatureObject> sample = points.takeSample(false, sampleSize);
        for (FeatureObject p : sample) {
        	quadTree.insertPoint(p);
        }   
        
        traverse(quadTree.getRoot());
        
        return quadTree;
	}
	
	// Create Global Index Quad Tree
	public QuadTree createQuadTreeLPT(double minX, double minY, double maxX, double maxY, 
			int samplePointsPerLeaf, int sampleSize, JavaRDD<FeatureObject> points, double radius) {
		
		// If a point is on the edges of the root's boundaries, it will throw an error. Adding a small padding
		double epsilon = 0.00001;
		
		QuadTree quadTree = new QuadTree(minX - epsilon, minY - epsilon, maxX + epsilon, maxY + epsilon, samplePointsPerLeaf);
		
		// sampling
        List<FeatureObject> sample = points.takeSample(false, sampleSize);
        
        for (FeatureObject p : sample) {
        	quadTree.insertPoint(p, radius);	        	
        }   
        
        traverse(quadTree.getRoot());
        
        return quadTree;
	}
	
	private void traverse(Node node) {
		if (node == null) {
			System.out.println("Null node");
			return;
		}
		if (node.hasChildrenQuadrants()) {
			Node[] children = node.getChildren();
			for (int i = 0; i < children.length; i++) {
				traverse(children[i]);
			}
		} else {
			insertToBins(node.getId(), node.getNumberOfContainedPoints());
		}
	}
	
	public QuadTree createQuadTree(double minX, double minY, double maxX, double maxY, int samplePointsPerLeaf, int sampleSize, JavaRDD<FeatureObject> points
			, double radius) {
		
		// If a point is on the edges of the root's boundaries, it will throw an error. Adding a small padding
		double epsilon = 0.00001;
		
		QuadTree quadTree = new QuadTree(minX - epsilon, minY - epsilon, maxX + epsilon, maxY + epsilon, samplePointsPerLeaf);
		
		// sampling
        List<FeatureObject> sample = points.takeSample(false, sampleSize);
        
        for (FeatureObject p : sample) {
        	quadTree.insertPoint(p, radius);
        }   
        
        return quadTree;
	}
	
	public RegularGrid createRegularGrid(double minX, double minY, double maxX, double maxY, int hSectors, int vSectors) {		
		return new RegularGrid(minX, minY, maxX, maxY, hSectors, vSectors);
	}
	
	// Map
    // Extract point information
	public JavaRDD<FeatureObject> mapToPoints(JavaRDD<String> lines, Broadcast<SpatioTextualJoin> broadcastStj) {
        JavaRDD<FeatureObject> points = lines.map(line -> {
        	SpatioTextualJoin stj = broadcastStj.getValue();
        	double longitude, latitude;
        	String[] objectKeywords;
        	int tag = Integer.parseInt(stj.extractWord(line, stj.tagIndex, stj.separator));
        	
        	if (tag == stj.file1Tag) {
        		longitude = Double.parseDouble(stj.extractWord(line, stj.file1LonIndex, stj.separator));
        		latitude = Double.parseDouble(stj.extractWord(line, stj.file1LatIndex, stj.separator));
        		objectKeywords = stj.extractWord(line, stj.keywordsIndex, stj.separator).split(stj.keywordsSeparator);
        		return new FeatureObject(longitude, latitude, tag, objectKeywords, 0);
        	} else if (tag == stj.file2Tag) {
        		longitude = Double.parseDouble(stj.extractWord(line, stj.file2LonIndex, stj.separator));
        		latitude = Double.parseDouble(stj.extractWord(line, stj.file2LatIndex, stj.separator));
        		objectKeywords = stj.extractWord(line, stj.keywordsIndex, stj.separator).split(stj.keywordsSeparator);
        		return new FeatureObject(longitude, latitude, tag, objectKeywords, 0);
        	}
        	else {
        		throw new IllegalArgumentException();
        	}    	
        });
        
        return points.filter(point -> point != null);
	}
	
	// Map to pairs
	public JavaPairRDD<Integer, FeatureObject> map(JavaRDD<FeatureObject> points, Broadcast<? extends Object> broadcastSpatialIndex, 
			double radius) {		
		Object spatialIndex = broadcastSpatialIndex.getValue();
		
		JavaPairRDD<Integer, FeatureObject> pairs;
		
		if (spatialIndex.getClass() == QuadTree.class) {
			pairs = assignPointsToNodes(points, broadcastSpatialIndex, radius);
		} else if (spatialIndex.getClass() == RegularGrid.class) {
			pairs = assignPointsToCells(points, broadcastSpatialIndex, radius);
		} else {
			throw new IllegalArgumentException("Invalid spatial index provided.");
		}
		
		return pairs;
	}
	
	// Map to pairs
	public JavaPairRDD<Integer, FeatureObject> mapDuplicate(JavaRDD<FeatureObject> points, Broadcast<? extends Object> broadcastSpatialIndex, 
			double radius) {		
		Object spatialIndex = broadcastSpatialIndex.getValue();
		
		JavaPairRDD<Integer, FeatureObject> pairs;
		
		if (spatialIndex.getClass() == QuadTree.class) {
			pairs = assignPointsToNodesDuplicate(points, broadcastSpatialIndex, radius);
		} else if (spatialIndex.getClass() == RegularGrid.class) {
			pairs = assignPointsToCells(points, broadcastSpatialIndex, radius);
		} else {
			throw new IllegalArgumentException("Invalid spatial index provided.");
		}
		
		return pairs;
	}
		
	// Reduce
    // Produce result set (pairs of interest)
	public JavaRDD<Tuple2<Integer, Integer>> reduce(JavaPairRDD<Integer, List<FeatureObject>> pairs, double radius) {        		
        resultPairs = pairs.flatMap((FlatMapFunction<Tuple2<Integer, List<FeatureObject>>, Tuple2<Integer, Integer>>) pair -> {
        	
        	// output is used to hold result point pairs 
        	ArrayList<Tuple2<Integer, Integer>> output = new ArrayList<Tuple2<Integer, Integer>>();
        	
        	// Array list to retain data objects in memory
        	ArrayList<FeatureObject> local = new ArrayList<FeatureObject>();       	
        	
        	for (FeatureObject fo : pair._2) {
        		
        		// Load objects of "Left" dataset
        		if (fo.getTag() == 1) { 
        			local.add(fo);
        			continue;
        		}
        		
        		for (FeatureObject p : local) {			
    				//if (MathUtils.jaccardSimilarity(fo.getKeywords(), p.getKeywords()) > 0) {
    					// Check if it is within the provided distance
            			if (MathUtils.haversineDistance(p, fo) <= radius) {
    						output.add(new Tuple2<Integer, Integer>(p.getTag(), fo.getTag()));
            			}
    				//}
    				
        		}
        	}        	
        	
        	return output.iterator();
        });
        
        return resultPairs;
	}
	
	public JavaRDD<Tuple2<FeatureObject, FeatureObject>> reduceForTest(JavaPairRDD<Integer, List<FeatureObject>> pairs, double radius) {        		
		resultPairsTest = pairs.flatMap((FlatMapFunction<Tuple2<Integer, List<FeatureObject>>, Tuple2<FeatureObject, FeatureObject>>) pair -> {
        	
        	// output is used to hold result point pairs 
        	ArrayList<Tuple2<FeatureObject, FeatureObject>> output = new ArrayList<Tuple2<FeatureObject, FeatureObject>>();
        	
        	// Array list to retain data objects in memory
        	ArrayList<FeatureObject> local = new ArrayList<FeatureObject>();       	
        	
        	for (FeatureObject fo : pair._2) {
        		
        		// Load objects of "Left" dataset
        		if (fo.getTag() == 1) { 
        			local.add(fo);
        			continue;
        		}
        		
        		for (FeatureObject p : local) {			
    				//if (MathUtils.jaccardSimilarity(fo.getKeywords(), p.getKeywords()) > 0) {
    					// Check if it is within the provided distance
            			if (MathUtils.haversineDistance(p, fo) <= radius) {
    						output.add(new Tuple2<FeatureObject, FeatureObject>(p, fo));
            			}
    				//}
    				
        		}
        	}        	
        	
        	return output.iterator();
        });
        
        return resultPairsTest;
	}
	
	public JavaRDD<Tuple2<Integer, Double>> reduceJoinSelectivity(JavaPairRDD<Integer, List<FeatureObject>> pairs, double radius) {        		
        return pairs.map((Function<Tuple2<Integer, List<FeatureObject>>, Tuple2<Integer, Double>>) pair -> {
        	
        	// Array list to retain data objects in memory
        	ArrayList<FeatureObject> local = new ArrayList<FeatureObject>();       	
        	double numerator = 0;
        	int lDataset = 0;
        	int rDataset = 0;
        	for (FeatureObject fo : pair._2) {
        		
        		// Load objects of "Left" dataset
        		if (fo.getTag() == 1) { 
        			local.add(fo);
        			lDataset++;
        			continue;
        		}
        		
        		for (FeatureObject p : local) {			
        			rDataset++;
        			if (MathUtils.haversineDistance(p, fo) <= radius) {
						numerator++;
        			}
        		}
        	}        	
        	if (lDataset == 0 || rDataset == 0) {
        		return new Tuple2<Integer, Double>(pair._1, -1.0);
        	}
        	return new Tuple2<Integer, Double>(pair._1, numerator / (lDataset * rDataset));
        });
	}
	
	private JavaPairRDD<Integer, FeatureObject> assignPointsToNodes(JavaRDD<FeatureObject> points, Broadcast<? extends Object> broadcastSpatialIndex, 
			double radius) {
    	return points.flatMapToPair(point -> {
        	// Get broadcasted values 
        	QuadTree qt = (QuadTree) broadcastSpatialIndex.getValue();
        	if (point.getTag() == 1) {
        		ArrayList<Tuple2<Integer, FeatureObject>> result = qt.assignToLeafNodeIterator(qt.getRoot(), point);
        		return result.iterator();
        	} else {

				// else construct square around point with size length "radius" and center "point"   
            	// 0, 90, 180, 270 represents navigation bearing
            	double squareUpperY = MathUtils.getPointInDistanceAndBearing(point, radius, 0).getY();
            	double squareLowerY = MathUtils.getPointInDistanceAndBearing(point, radius, 180).getY();
            	double squareUpperX = MathUtils.getPointInDistanceAndBearing(point, radius, 90).getX();
            	double squareLowerX = MathUtils.getPointInDistanceAndBearing(point, radius, 270).getX();
            	
            	point.setSquare(squareLowerX, squareLowerY, squareUpperX, squareUpperY);
            	
            	// Assign point to every leaf that intersects with the square
            	ArrayList<Tuple2<Integer, FeatureObject>> result = qt.assignToLeafNodeAndDuplicate(qt.getRoot(), point);

            	return result.iterator();
        	}
        });	
	}
	
	private JavaPairRDD<Integer, FeatureObject> assignPointsToNodesDuplicate(JavaRDD<FeatureObject> points, Broadcast<? extends Object> broadcastSpatialIndex, 
			double radius) {
    	return points.flatMapToPair(point -> {
        	// Get broadcasted values 
        	QuadTree qt = (QuadTree) broadcastSpatialIndex.getValue();
        	
        	Node enclosingNode = qt.getEnclosingNode(qt.getRoot(), point);
        	ArrayList<Tuple2<Integer, FeatureObject>> result = new ArrayList<Tuple2<Integer, FeatureObject>>();
        	//result.add(new Tuple2<Integer, FeatureObject>(enclosingNode.getId(), point));
        	
        	// if the cell is RED and the point is RED  (OR) the cell is BLUE and the point is BLUE, only then duplicate
        	if ((enclosingNode.duplicateLeftDataset() && point.getTag() == 1) || (!enclosingNode.duplicateLeftDataset() && point.getTag() == 2)) {
        		// 0, 90, 180, 270 represents navigation bearing
            	double squareUpperY = MathUtils.getPointInDistanceAndBearing(point, radius, 0).getY();
            	double squareLowerY = MathUtils.getPointInDistanceAndBearing(point, radius, 180).getY();
            	double squareUpperX = MathUtils.getPointInDistanceAndBearing(point, radius, 90).getX();
            	double squareLowerX = MathUtils.getPointInDistanceAndBearing(point, radius, 270).getX();
            	
            	point.setSquare(squareLowerX, squareLowerY, squareUpperX, squareUpperY);
            	result.addAll(qt.duplicateToLeafNodes(qt.getRoot(), enclosingNode.getId(), enclosingNode.duplicateLeftDataset(), point));
        	}
        	
        	
        	return result.iterator();
        });	
	}
	
	private JavaPairRDD<Integer, FeatureObject> assignPointsToCells(JavaRDD<FeatureObject> points, Broadcast<? extends Object> broadcastSpatialIndex, 
			double radius) {
		return points.flatMapToPair(point -> {
        	// Get broadcasted values 
        	RegularGrid grid = (RegularGrid) broadcastSpatialIndex.getValue();
        	if (point.getTag() == 1) {
        		ArrayList<Tuple2<Integer, FeatureObject>> result = grid.assignToCellIterator(point);
        		return result.iterator();
        	} else {
        		ArrayList<Tuple2<Integer, FeatureObject>> result = grid.assignToCellAndDuplicate(point, radius);
            	return result.iterator();
        	}            	
        });
    }
}