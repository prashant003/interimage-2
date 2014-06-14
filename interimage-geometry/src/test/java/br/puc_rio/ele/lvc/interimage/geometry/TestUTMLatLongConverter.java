package br.puc_rio.ele.lvc.interimage.geometry;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;

import junit.framework.TestCase;

public class TestUTMLatLongConverter extends TestCase {

	@Test
	public void testUTMLatLongConverter() {
		UTMLatLongConverter converter = new UTMLatLongConverter();		
		assertEquals(true, converter.hasDatum("WGS84"));
		assertEquals(6378137.0, converter.getDatum("WGS84")[0]);
		assertEquals(298.257223563, converter.getDatum("WGS84")[1]);
	}

	@Test
	public void testSetDatum() {
		UTMLatLongConverter converter = new UTMLatLongConverter();
		converter.setDatum("WGS84");
		assertEquals(6378137.0, converter.getEquatorialRadius(), 1E-15);
		assertEquals(1.0 / 298.257223563, converter.getPolarFlattening(), 1E-15);
		assertEquals(6378137.0 * (1.0 - (1.0 / 298.257223563)), converter.getPolarRadius(), 1E-15);
		assertEquals(Math.sqrt(1.0 - Math.pow(6378137.0 * (1.0 - (1.0 / 298.257223563)), 2.0) / Math.pow(6378137.0, 2.0)), converter.getEccentricity(), 1E-15);
	}

	@Test
	public void testLatLongToUTM() {
		UTMLatLongConverter converter = new UTMLatLongConverter();
		converter.setDatum("WGS84");
		Coordinate coord = new Coordinate(-92.284639707529, 44.560484263791);
		converter.LatLongToUTM(coord);
		assertEquals(556810.733051, coord.x, 1E-3);
		assertEquals(4934376.512002, coord.y, 1E-3);
	}

	@Test
	public void testUTMToLatLong() {
		UTMLatLongConverter converter = new UTMLatLongConverter();
		converter.setDatum("WGS84");
		Coordinate coord = new Coordinate(556810.733051, 4934376.512002);
		converter.UTMToLatLong(coord, 15, false);
		assertEquals(-92.284639707529, coord.x, 1E-9);
		assertEquals(44.560484263791, coord.y, 1E-9);
	}

}
