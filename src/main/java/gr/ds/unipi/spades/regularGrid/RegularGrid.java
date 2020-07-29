package gr.ds.unipi.spades.regularGrid;

import java.util.ArrayList;

import gr.ds.unipi.spades.geometry.Point;
import scala.Tuple2;

public class RegularGrid {
	private double minX, minY, maxX, maxY;
	private int hSectors, vSectors;
	private Cell[][] cells;
	
	public Cell[][] getCells() {
		return cells;
	}
	
	// Example of a 3x3 regular grid
	//  -------------------------------------------------------------
    //  |					|					|					|   
    //  |    	6			|		7			|		8			|					
    //  |					|					|					|
    //  -------------------------------------------------------------
    //  |					|					|					|   
    //  |    	3			|		4			|		5			|					
    //  |					|					|					|
    //  -------------------------------------------------------------
    //  |					|					|					|   
    //  |    	0			|		1			|		2			|					
    //  |					|					|					|
    //  -------------------------------------------------------------
    // The numbers represent the cell id. 
    // Cell[3][3] array is translated as follows:
    // [0][0] --> 0
    // [0][1] --> 1
    // [0][2] --> 2
    // [1][0] --> 3
    // ............
    // [2][1] --> 7
    // [2][2] --> 8
	
	public RegularGrid(double minX, double minY, double maxX, double maxY, int hSectors, int vSectors) {
		this.minX = minX;
		this.minY = minY;
		this.maxX = maxX;
		this.maxY = maxY;
		this.hSectors = hSectors;
		this.vSectors = vSectors;
		
		cells = new Cell[this.vSectors][this.hSectors];
		
		double hStep = (this.maxX - this.minX) / this.hSectors;
		double vStep = (this.maxY - this.minY) / this.vSectors;		
		short id = 0;
		for (int i = 0; i < vSectors; i++) {
			for (int j = 0; j < hSectors; j++) {
				// the three following cases ensure that there won't be any
				// calculation errors (caused by floating-point precision) that will
				// result in creating a grid slightly smaller than intended
				
				// case when building top-right cell
				if (j == hSectors - 1 && i == vSectors - 1) {
					cells[i][j] = new Cell(this.minX + j * hStep, this.minY + i * vStep,
							maxX, maxY, id++);
					continue;
				} 
				
				// case when building cells on the right side of the grid
				if (j == hSectors -1) {
					cells[i][j] = new Cell(this.minX + j * hStep, this.minY + i * vStep,
							maxX, minY + (i + 1) * vStep, id++);
					continue;
				}
				
				
				// case when building cells on the top side of the grid
				if (i == vSectors - 1) {
					cells[i][j] = new Cell(this.minX + j * hStep, this.minY + i * vStep,
							minX + (j + 1) * hStep, maxY, id++);
					continue;
				}
				
				// case when building the remaining cells
				cells[i][j] = new Cell(this.minX + j * hStep, this.minY + i * vStep,
						minX + (j + 1) * hStep, minY + (i + 1) * vStep, id++);
			}
		}
	}
	
	public ArrayList<Tuple2<Integer, Point>> assignToCellIterator(Point point) {
		int j = getColumnIndex(point);
		int i = getRowIndex(point);
		
		ArrayList<Tuple2<Integer, Point>> pair = new ArrayList<Tuple2<Integer, Point>>();
    	pair.add(new Tuple2<Integer, Point>(cells[i][j].getId(), point));
    	return pair;
	}
	
	public ArrayList<Tuple2<Integer, Point>> assignToCellAndDuplicate(Point point, double radius) {
		int j = getColumnIndex(point);
		int i = getRowIndex(point);		
		ArrayList<Tuple2<Integer, Point>> pairs = new ArrayList<Tuple2<Integer, Point>>();
		
		pairs.add(new Tuple2<Integer, Point>(cells[i][j].getId(), point));
		
		if (i == 0 && j == 0) {
			if (cells[i + 1][j].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j].getId(), point));
			}
			
			if (cells[i][j + 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i][j + 1].getId(), point));
			}
			
			if (cells[i + 1][j + 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j + 1].getId(), point));
			}
			return pairs;
		}
		
		if (i == 0 && j == cells[0].length - 1) {
			if (cells[i + 1][j].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j].getId(), point));
			}
			
			if (cells[i][j - 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i][j - 1].getId(), point));
			}
			
			if (cells[i + 1][j - 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j - 1].getId(), point));
			}
			return pairs;
		}
		
		if (i == 0) {
			if (cells[i][j - 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i][j - 1].getId(), point));
			}
			
			if (cells[i][j + 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i][j + 1].getId(), point));
			}
			
			if (cells[i + 1][j - 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j - 1].getId(), point));
			}
			
			if (cells[i + 1][j].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j].getId(), point));
			}
			
			if (cells[i + 1][j + 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j + 1].getId(), point));
			}
			return pairs;
		}
		
		// ------------------------------------------
		
		if (i == cells.length - 1 && j == 0) {
			if (cells[i - 1][j].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j].getId(), point));
			}
			
			if (cells[i][j + 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i][j + 1].getId(), point));
			}
			
			if (cells[i - 1][j + 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j + 1].getId(), point));
			}
			return pairs;
		}
		
		if (i == cells.length - 1 && j == cells[0].length - 1) {			
			if (cells[i - 1][j].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j].getId(), point));
			}
			
			if (cells[i][j - 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i][j - 1].getId(), point));
			}
			
			if (cells[i - 1][j - 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j - 1].getId(), point));
			}
			return pairs;
		}
		
		if (i == cells.length - 1) {
			if (cells[i][j - 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i][j - 1].getId(), point));
			}
			
			if (cells[i][j + 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i][j + 1].getId(), point));
			}
			
			if (cells[i - 1][j - 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j - 1].getId(), point));
			}
			
			if (cells[i - 1][j].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j].getId(), point));
			}
			
			if (cells[i - 1][j + 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j + 1].getId(), point));
			}
			return pairs;
		}
		
		if (j == 0) {
			if (cells[i + 1][j].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j].getId(), point));
			}
			
			if (cells[i - 1][j].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j].getId(), point));
			}
			
			if (cells[i - 1][j + 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j + 1].getId(), point));
			}
			
			if (cells[i + 1][j + 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j + 1].getId(), point));
			}
			
			if (cells[i][j + 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i][j + 1].getId(), point));
			}
			
			return pairs;
		}
		
		if (j == cells[0].length - 1) {
			if (cells[i + 1][j].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j].getId(), point));
			}
			
			if (cells[i - 1][j].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j].getId(), point));
			}
			
			if (cells[i - 1][j - 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j - 1].getId(), point));
			}
			
			if (cells[i + 1][j - 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j - 1].getId(), point));
			}
			
			if (cells[i][j - 1].isInDistance(point, radius)) {
				pairs.add(new Tuple2<Integer, Point>(cells[i][j - 1].getId(), point));
			}
			
			return pairs;
		}
		
		if (cells[i + 1][j].isInDistance(point, radius)) {
			pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j].getId(), point));
		}
		
		if (cells[i - 1][j].isInDistance(point, radius)) {
			pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j].getId(), point));
		}
		
		if (cells[i - 1][j - 1].isInDistance(point, radius)) {
			pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j - 1].getId(), point));
		}
		
		if (cells[i + 1][j - 1].isInDistance(point, radius)) {
			pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j - 1].getId(), point));
		}
		
		if (cells[i][j - 1].isInDistance(point, radius)) {
			pairs.add(new Tuple2<Integer, Point>(cells[i][j - 1].getId(), point));
		}
		
		if (cells[i][j + 1].isInDistance(point, radius)) {
			pairs.add(new Tuple2<Integer, Point>(cells[i][j + 1].getId(), point));
		}
		
		if (cells[i - 1][j + 1].isInDistance(point, radius)) {
			pairs.add(new Tuple2<Integer, Point>(cells[i - 1][j + 1].getId(), point));
		}
		
		if (cells[i + 1][j + 1].isInDistance(point, radius)) {
			pairs.add(new Tuple2<Integer, Point>(cells[i + 1][j + 1].getId(), point));
		}
		
		return pairs;
	}
	
	private int getRowIndex(Point point) {
		int i = (int) Math.floor(vSectors * (point.getY() - minY) / (maxY - minY));
		if (i == vSectors) {
			i--;
		}
		return i;
	}
	
	private int getColumnIndex(Point point) {
		int j = (int) Math.floor(hSectors * (point.getX() - minX) / (maxX - minX));
		if (j == hSectors) {
			j--;
		}		
		return j;
	}
}
