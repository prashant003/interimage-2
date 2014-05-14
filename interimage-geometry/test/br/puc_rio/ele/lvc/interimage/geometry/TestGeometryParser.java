package br.puc_rio.ele.lvc.interimage.geometry;

import org.apache.pig.data.DataByteArray;
import org.junit.Test;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

import junit.framework.TestCase;

public class TestGeometryParser extends TestCase {

	@Test
	public void testParseGeometryString() {
		
		try {
			
			GeometryParser parser = new GeometryParser();
			Geometry geometryStr = null;
			
			String geoStr = "POLYGON ((684306.2852457707 7463168.402618, 684308.2853693123 7463168.402618, 684309.2854310831 7463168.402618, 684309.2854310831 7463167.402556229, 684308.2853693123 7463167.402556229, 684306.2852457707 7463167.402556229, 684306.2852457707 7463168.402618))"; 
			geometryStr = parser.parseGeometry(geoStr);
			
			Geometry geometry = new WKTReader().read(geoStr);
			
			assertTrue(geometry.equals(geometryStr));
			
		} catch (Exception e) {
			System.err.println("Failed to parse geometry; error - " + e.getMessage());
		}
		
	}

	@Test
	public void testParseGeometryBinary() {
		
		try {
			
			GeometryParser parser = new GeometryParser();
			Geometry geometryBinary = null;
			
			String geoStr = "POLYGON ((684306.2852457707 7463168.402618, 684308.2853693123 7463168.402618, 684309.2854310831 7463168.402618, 684309.2854310831 7463167.402556229, 684308.2853693123 7463167.402556229, 684306.2852457707 7463167.402556229, 684306.2852457707 7463168.402618))"; 
			
			Geometry geometryStr = new WKTReader().read(geoStr);
			
			byte[] bytes = new WKBWriter().write(geometryStr);
			
			DataByteArray array = new DataByteArray(bytes);
			
			geometryBinary = parser.parseGeometry(array);
			
			Geometry geometry = new WKTReader().read(geoStr);
			
			assertTrue(geometry.equals(geometryBinary));
			
		} catch (Exception e) {
			System.err.println("Failed to parse geometry; error - " + e.getMessage());
		}
		
	}
	
	@Test
	public void testParseGeometryHexString() {
		
		try {
			
			GeometryParser parser = new GeometryParser();
			Geometry geometryBinary = null;
			
			String geoStr = "POLYGON ((684306.2852457707 7463168.402618, 684308.2853693123 7463168.402618, 684309.2854310831 7463168.402618, 684309.2854310831 7463167.402556229, 684308.2853693123 7463167.402556229, 684306.2852457707 7463167.402556229, 684306.2852457707 7463168.402618))"; 
			
			Geometry geometryStr = new WKTReader().read(geoStr);
			
			byte[] bytes = new WKBWriter().write(geometryStr);
						
			geometryBinary = parser.parseGeometry(WKBWriter.bytesToHex(bytes));
			
			Geometry geometry = new WKTReader().read(geoStr);
			
			assertTrue(geometry.equals(geometryBinary));
			
		} catch (Exception e) {
			System.err.println("Failed to parse geometry; error - " + e.getMessage());
		}
		
	}
	
}
