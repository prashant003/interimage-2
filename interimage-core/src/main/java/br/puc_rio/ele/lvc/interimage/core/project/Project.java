/*Copyright 2014 Computer Vision Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

package br.puc_rio.ele.lvc.interimage.core.project;

import br.puc_rio.ele.lvc.interimage.common.TileManager;
import br.puc_rio.ele.lvc.interimage.common.URL;
import br.puc_rio.ele.lvc.interimage.core.datamanager.DataManager;
import br.puc_rio.ele.lvc.interimage.core.datamanager.DefaultResource;
import br.puc_rio.ele.lvc.interimage.core.datamanager.SplittableResource;
import br.puc_rio.ele.lvc.interimage.core.semanticnetwork.SemanticNetwork;
import br.puc_rio.ele.lvc.interimage.data.Image;
import br.puc_rio.ele.lvc.interimage.data.ImageList;
import br.puc_rio.ele.lvc.interimage.datamining.FuzzySet;
import br.puc_rio.ele.lvc.interimage.datamining.FuzzySetList;
import br.puc_rio.ele.lvc.interimage.geometry.Shape;
import br.puc_rio.ele.lvc.interimage.geometry.ShapeList;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

/**
 * A class that holds the information about an interpretation project. 
 * @author Rodrigo Ferreira
 * 
 * TODO: preprocess net file: 1) replace "topdown/", 2) get rid of decision rules and 3) set crs code
 */
public class Project {

	private String _projectPath;
	private String _projectName;
	private SemanticNetwork _semanticNet;
	private ImageList _imageList;
	private ShapeList _shapeList;
	private DataManager _dataManager;
	private double _minResolution;
	//TODO: Make it a parameter
	private int _tilePixelSize;
	private TileManager _tileManager;
	private FuzzySetList _fuzzySetList;
	//private String _decisionTree = null;
	private Properties _properties;
	//private RuleSet _ruleSet;
	
	public Project() {
		_semanticNet = new SemanticNetwork();
		_imageList = new ImageList();
		_shapeList = new ShapeList();
		_dataManager = new DataManager();
		_fuzzySetList = new FuzzySetList();
		_minResolution = Double.MAX_VALUE;
		_properties = new Properties();
		//_ruleSet = new RuleSet();		
	}
	
	public String getProjectPath() {
		return _projectPath;
	}
	
	public SemanticNetwork getSemanticNetwork() {
		return _semanticNet;
	}
	
	public ImageList getImageList() {
		return _imageList;
	}
	
	public ShapeList getShapeList() {
		return _shapeList;
	}
	
	public void readOldFile(String url) {
	
		try {
		
			/*Processing input parameters*/
			if (url == null) {
	            throw new Exception("No project file specified");
	        } else {
	        	if (url.isEmpty()) {
	        		throw new Exception("No project file specified");
	        	}
	        }
						
			_projectPath = url;
			
			_properties.setProperty("interimage.projectPath", _projectPath);
			
			_projectName = URL.getFileNameWithoutExtension(url);
			
			_properties.setProperty("interimage.projectName", _projectName);
			
			/*Reading properties file*/
			InputStream input = new FileInputStream("interimage.properties");

			_properties.load(input);
			
			/*Setting reduce parallelism*/
			int clusterSize = Integer.parseInt(_properties.getProperty("interimage.clusterSize"));
			int parallel = (int)Math.round(clusterSize * 0.8);
			
			_properties.setProperty("interimage.parallel", String.valueOf(parallel));

			_tilePixelSize = Integer.parseInt(_properties.getProperty("interimage.tileSize"));
			
			_dataManager.setup(_properties);
			
			_properties.setProperty("interimage.sourceURL", _dataManager.getSourceURL());
			_properties.setProperty("interimage.sourceSpecificURL", _dataManager.getSourceSpecificURL());
			
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			
			InputStream in = new FileInputStream(url);
			
			Document doc = dBuilder.parse(in);
		      
		    Element rootElement = doc.getDocumentElement();
		    rootElement.normalize();
			
		    if (rootElement.getNodeName() == "geoproject") {
		    			    	
		    	NodeList semNets = rootElement.getElementsByTagName("geosemnet");
			    NodeList images = rootElement.getElementsByTagName("image");
			    NodeList shapes = rootElement.getElementsByTagName("shape");
			    NodeList fuzzySets = rootElement.getElementsByTagName("fuzzysets");			    
		    				    
			    /*Reading Semantic Network*/
			    if (semNets.getLength() > 0) {
			    	
			    	Element semNet = (Element)semNets.item(0);
			    	
			    	_semanticNet.readOldFile(semNet.getAttribute("dir") + File.separatorChar + semNet.getAttribute("file"));
			    	
			    	//TODO: send semantic network to AWS
			    	
			    } else {
			    	throw new Exception("No geosemnet tag defined");
			    }
			    
			    /*Reading Image List*/
			    if (images.getLength() > 0) {
			    				    	
			    	String crs = null;
			    	
			    	for (int k = 0; k < images.getLength(); k++) {
				    	
			    		Node imageNode = images.item(k);
			    					    		
			    		if (imageNode.getNodeType() == Node.ELEMENT_NODE) {
			    			
				    		Element image = (Element)imageNode;

					    	boolean isEnabled = Boolean.parseBoolean(image.getAttribute("enabled"));
					    	
					    	if (!isEnabled)
					    		continue;
				    		
					    	Image img = new Image();
					    	
					    	String key = image.getAttribute("key");				    	
					    	
					    	boolean isDefault = Boolean.parseBoolean(image.getAttribute("default"));
					    	
					    	img.setKey(key);
					    	img.setDefault(isDefault);
					    	img.setURL(image.getAttribute("file"));
						    					    	
					    	String crsFrom = image.getAttribute("crs");
					    	
					    	if (isDefault) {
					    		crs = crsFrom;
					    	}
					    	
					    	img.setCRS(crsFrom);
					    	
					    	//TODO: Check if a conversion should be done here
					    			
					    	Point coord1 = new GeometryFactory().createPoint(new Coordinate(Double.parseDouble(image.getAttribute("geoWest")), Double.parseDouble(image.getAttribute("geoSouth"))));
					        
					        Point coord2 = new GeometryFactory().createPoint(new Coordinate(Double.parseDouble(image.getAttribute("geoEast")), Double.parseDouble(image.getAttribute("geoNorth"))));
					    	
					        //CRS.convert(crsFrom, crsFrom, coord1);
					        //CRS.convert(crsFrom, crsFrom, coord2);
					        
							img.setGeoWest(coord1.getX());
							img.setGeoSouth(coord1.getY());
							img.setGeoEast(coord2.getX());					    	
					    	img.setGeoNorth(coord2.getY());
							
					    	/*System.out.println(coord1.x);
					        System.out.println(coord1.y);
					        System.out.println(coord2.x);
					        System.out.println(coord2.y);*/
					    	
					    	img.setCols(Integer.parseInt(image.getAttribute("cols")));
					    	img.setRows(Integer.parseInt(image.getAttribute("rows")));
					    	img.setBands(Integer.parseInt(image.getAttribute("bands")));
					    		
					    	double res = Math.abs((img.getGeoEast()-img.getGeoWest())/img.getCols());
					    	res = Math.min(res, Math.abs((img.getGeoSouth()-img.getGeoNorth())/img.getRows()));
					    						    	
					    	if (res < _minResolution) {
					    		_minResolution = res; 
					    	}
					    						    	
					    	_imageList.add(key, img);
					    	
			    		}
			    	
			    	}
			    			
			    	_properties.setProperty("interimage.crs", crs);
			    	
			    	_properties.setProperty("interimage.tileSizeMeters", String.valueOf(_tilePixelSize * _minResolution));
			    				    	
			    	//System.out.println(_properties.getProperty("interimage.tileSize"));
			    	
			    	_tileManager = new TileManager(_tilePixelSize * _minResolution, crs);
			    	
			    	_dataManager.updateGeoBBox(new double[] {_imageList.getGeoWest(), _imageList.getGeoSouth(), _imageList.getGeoEast(), _imageList.getGeoNorth()}); 
			    	
			    	for (Map.Entry<String, Image> entry : _imageList.getImages().entrySet()) {
			    		_dataManager.setupResource(new SplittableResource(entry.getValue(),SplittableResource.IMAGE), _tileManager, _projectName, URL.getPath(_projectPath));
			    	}
			    				    	
			    } else {
			    	throw new Exception("No image tag defined");
			    }
			    			    
			    /*Reading Shape List*/
			    if (shapes.getLength() > 0) {
			    				    	
			    	for (int k = 0; k < shapes.getLength(); k++) {
				    	Element shape = (Element)shapes.item(k);
				    	
				    	Shape shp = new Shape();
				    	String key = shape.getAttribute("key");				    	
				    	
				    	shp.setKey(key);
				    	shp.setURL(shape.getAttribute("file"));
				    	
				    	String crsFrom = shape.getAttribute("crs");				    	
				    	shp.setCRS(crsFrom);
				    	
				    	Boolean splittable = new Boolean(shape.getAttribute("splittable"));				    	
				    	shp.isSplittable(splittable);
				    	
				    	_shapeList.add(key, shp);
				    	
				    	if (splittable) {
				    		_dataManager.setupResource(new SplittableResource(shp,SplittableResource.SHAPE), _tileManager, _projectName, null);
				    	} else {
				    		_dataManager.setupResource(new DefaultResource(shp,DefaultResource.SHAPE), null, _projectName, null);
				    	}
			    	}
			    				    	
			    } else {
			    	System.out.println("Warning: No shape tag defined in your project file.");
			    }
			    	
			    /*Creating Tiles*/
			    _tileManager.setTiles(_dataManager.getGeoBBox());
			    
			    String tileUrl = null;
			    
			    tileUrl = _dataManager.setupResource(new DefaultResource(_tileManager.getTiles(), DefaultResource.TILE), _tileManager, _projectName, URL.getPath(_projectPath));
			    		
			    _properties.setProperty("interimage.tileUrl", tileUrl);
			    
			    String fuzzyUrl = null;
			    
			    /*Reading FuzzySets*/
			    if (fuzzySets.getLength() > 0) {
			    	
			    	Element fuzzySet = (Element)fuzzySets.item(0);
			    	
			    	_fuzzySetList.readOldFile(fuzzySet.getAttribute("file"));

			    	if (_fuzzySetList.size()>0)
			    		fuzzyUrl = _dataManager.setupResource(new DefaultResource(new ArrayList<FuzzySet>(_fuzzySetList.getFuzzySets().values()), DefaultResource.FUZZY_SET), null, _projectName, URL.getPath(_projectPath));			    	
			    	
			    } else {
			    	System.out.println("Warning: No fuzzysets tag defined in your project file.");
			    }
			    
			    _properties.setProperty("interimage.fuzzyUrl", fuzzyUrl);
			    
			    //_ruleSet.setup(_properties);
			    
			    //Test			    
			    //_ruleSet.readOldFile("C:\\Users\\Rodrigo\\Desktop\\test.dt");
			    //System.out.println(_ruleSet.getPigCode());
			    //Test
			    
			    /*Sending libs to the cluster*/			    
			    File folder = new File("lib");
				
				for (final File fileEntry : folder.listFiles()) {
			        if (fileEntry.isDirectory()) {
			        	//ignore
			        } else {
			        	_dataManager.setupResource(new DefaultResource(new String("lib/" + fileEntry.getName()), DefaultResource.FILE), null, _projectName, null);	
			        }
			    }
			    
			    /*Sending import file to the cluster*/
			    _dataManager.setupResource(new DefaultResource(new String("interimage-import.pig"), DefaultResource.FILE), null, _projectName, null);
			    			    
		    } else {
		    	throw new Exception("No geoproject tag defined");
		    }
		    
		    _dataManager.close();
		    
		} catch (Exception e) {
			System.err.println("Failed to read project file; error - " + e.getMessage());
			e.printStackTrace();
		}
		
	}
	
}
