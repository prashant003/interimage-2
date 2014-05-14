package br.puc_rio.ele.lvc.interimage.geometry;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;

import junit.framework.TestCase;

public class TestWebMercatorLatLongConverter extends TestCase {

	@Test
	public void testWebMercatorLatLongConverter() {
		WebMercatorLatLongConverter converter = new WebMercatorLatLongConverter();		
		assertEquals(true, converter.hasDatum("WGS84"));
		assertEquals(6378137.0, converter.getDatum("WGS84")[0]);
		assertEquals(298.257223563, converter.getDatum("WGS84")[1]);
	}

	@Test
	public void testSetDatum() {
		WebMercatorLatLongConverter converter = new WebMercatorLatLongConverter();
		converter.setDatum("WGS84");
		assertEquals(6378137.0, converter.getEquatorialRadius());
	}

	@Test
	public void testLatLongToWebMercator() {	//data got from http://spatialreference.org
		WebMercatorLatLongConverter converter = new WebMercatorLatLongConverter();
		converter.setDatum("WGS84");
		Coordinate coord = new Coordinate(-50.39428710937, -11.810302734375);
		converter.LatLongToWebMercator(coord);
		assertEquals(-5609866.379905, coord.x, 1E-3);
		assertEquals(-1324127.19247, coord.y, 1E-3);
	}

	@Test
	public void testWebMercatorToLatLong() { // data got from http://spatialreference.org
		WebMercatorLatLongConverter converter = new WebMercatorLatLongConverter();
		converter.setDatum("WGS84");
		Coordinate coord = new Coordinate(-5609866.379905, -1324127.19247);
		converter.WebMercatorToLatLong(coord);
		assertEquals(-50.39428710937, coord.x, 1E-10);
		assertEquals(-11.810302734375, coord.y, 1E-10);
	}

}
