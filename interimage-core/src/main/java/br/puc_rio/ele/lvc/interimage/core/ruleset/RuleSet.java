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

package br.puc_rio.ele.lvc.interimage.core.ruleset;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import br.puc_rio.ele.lvc.interimage.common.Common;
import br.puc_rio.ele.lvc.interimage.core.udf.UDFSet;

/**
 * A class that holds the information about a rule set. 
 * @author Rodrigo Ferreira
 */
public class RuleSet {

	private String _url;
	private Rule _root;
	
	//private String _lastRelation;
	//private String _lastClassRelation;
	//private Map<String,Integer> _counts;
	private List<String> _spectralCalculations;
	private boolean _firstClass = true;
	//private Properties _properties;
	private UDFSet _udfSet;
	
	//TODO: Must become parameters
	//private int _parallel;
		
	public RuleSet() {
		/*_counts = new HashMap<String,Integer>();
		_counts.put("projection", 1);
		_counts.put("selection", 1);
		_counts.put("group", 1);
		_counts.put("load", 1);
		_counts.put("union", 1);
		_lastRelation = "undefined";
		_lastClassRelation = "undefined";*/
		
		_spectralCalculations = new ArrayList<String>();
				
		_udfSet = new UDFSet();
		_udfSet.loadUDFs();
		
	}
	
	/*public void setup(Properties properties) {
		_properties = properties;
		//_parallel = Integer.parseInt(_properties.getProperty("interimage.parallel"));		
	}*/
	
	/*public void setLastRelation(String relation) {
		String[] terms = relation.split("_");
		_lastRelation = terms[0] + "_" + (Integer.parseInt(terms[1]) + 1);
	}
		
	public void setCounts(Map<String, List<String>> relationMap) {
		for (Map.Entry<String, List<String>> entry : relationMap.entrySet()) {			
			List<String> list = (List<String>)entry.getValue();
			_counts.put(entry.getKey(), list.size()+1);
		}
	}*/
	
	public void setRootRule(Rule rule) {
		_root = rule;
	}
	
	public Rule getRootRule() {
		return _root;
	}
	
	private void processRule(Rule rule, NodeList children) {
		
		if (rule == null)
			return;
		
		if (children == null) {
			return;
		}
		
    	if (children.getLength() > 0) {
	    	
    		for (int k = 0; k < children.getLength(); k++) {
	    		    
    			Node ruleNode = children.item(k);
    			    			
    			if (ruleNode.getNodeType() == Node.ELEMENT_NODE) {
    			
			    	Element n = (Element)ruleNode;	
			    		    	
			    	Rule sRule = new Rule();
			    	sRule.setParent(rule);
			    	sRule.setType(n.getAttribute("type"));
			    	sRule.setLabel(n.getAttribute("label"));
			    	sRule.setCommented(n.getAttribute("commented"));
			    	sRule.setScope(n.getAttribute("scope"));
			    				    	 
			    	rule.addChildRule(sRule);
			    	
			    	processRule(sRule, n.getChildNodes());
			    	
    			}
		    	
    		}
	    	    		
	    }
		
	}
	
	public void readOldFile(String url) {
		
		try {
		
			/*Processing input parameters*/
			if (url == null) {
	            throw new Exception("No rule set file specified");
	        } else {
	        	if (url.isEmpty()) {
	        		throw new Exception("No rule set file specified");
	        	}
	        }
			
			_url = url;
						
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			
			InputStream in = new FileInputStream(url);
			
			Document doc = dBuilder.parse(in);
		      
		    Element rootElement = doc.getDocumentElement();
		    rootElement.normalize();
		    
		    _root = new Rule();
	    	_root.setParent(null);
	    	_root.setType(rootElement.getAttribute("type"));
	    	_root.setLabel(rootElement.getAttribute("label"));
	    	_root.setCommented(rootElement.getAttribute("commented"));
	    	_root.setScope(rootElement.getAttribute("scope"));
	    	processRule(_root, rootElement.getChildNodes());	
			
		} catch (Exception e) {
			System.err.println("Failed to read rule set file; error - " + e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	/*private String nextRelation(String type) {
		int count = _counts.get(type);
		_counts.put(type,count+1);
		String result = type + "_" + count;		
		return result;
	}*/
		
	public String parseExpression(String expression) {
		String result = null;
		if (_udfSet.getUDFs().containsKey(expression)) {
			result = _udfSet.getUDFs().get(expression) + "(geometry)";
		} else {
			
			expression = expression.trim();
					
			expression = expression.replace("(", " ( ");
			expression = expression.replace(")", " ) ");
			expression = expression.replace("+", " + ");
			expression = expression.replace("/", " / ");
			expression = expression.replace("*", " * ");
			/* difference signal must have spaces in the decision rule*/
			//expression = expression.replace("-", " - ");
			
			String [] tokens = expression.split(" ");
			
			for (int i=0; i<tokens.length; i++) {
				
				tokens[i] = tokens[i].trim();
				
				if ((!tokens[i].equals("+"))
					&& (!tokens[i].equals("-"))
					&& (!tokens[i].equals("/"))
					&& (!tokens[i].equals("*"))
					&& (!tokens[i].equals("("))
					&& (!tokens[i].equals(")"))
					)
				{
					if (!tokens[i].isEmpty())
						if (!Common.isNumeric(tokens[i]))
							tokens[i] = "properties#'" + tokens[i] + "'";
				}
				
			}
			
			StringBuilder builder = new StringBuilder();
			for(String s : tokens) {
				if (!s.isEmpty())
					builder.append(s);
			}
			result = builder.toString();
		}
		return result;
	}

	private void loadSpectral(Rule rule, StringBuilder code) {
		
		checkSpectral(rule);
		
		if (_spectralCalculations.size()>0) {
			
			String list = "";			
			boolean first = true;
			
			for (String s : _spectralCalculations) {
				if (first) {
					list += s.replace("'", "");
					first = false;
				} else {
					list += ";" + s.replace("'", "");
				}
			}
			
			//TODO: Maybe a parameter could define the way spectral features will be considered
			//String relation = nextRelation("group");
			code.append("DEFINE SpectralFeatures br.puc_rio.ele.lvc.interimage.data.udf.SpectralFeatures('$IMAGES_PATH','" + list + "','$TILE_SIZE_METERS');\n");
			code.append("group = II_SpectralFeatures($LAST_RELATION, $PARALLEL);\n");			
			//_lastRelation = relation;
						
		}
		
		_spectralCalculations.clear();
		
	}
	
	private void evaluateRule(Rule rule, StringBuilder code) {
		
		if (rule.getType().equals("Union")) {
			
			boolean first = true;
			
			List<String> loads = new ArrayList<String>();
			
			for (int k=0; k<rule.getChildren().size(); k++) {
				
				if (first) {					
					evaluateRule(rule.getChildren().get(k), code);
					//loads.add(_lastRelation);
					first = false;
				} else {					
					evaluateRule(rule.getChildren().get(k), code);
					//loads.add(_lastRelation);					
				}
							
			}
			
			code.append("\n");
			
			/*String relation = nextRelation("group");
			code.append(relation + " = COGROUP ");
			
			first = true;
			for (int j=0; j<loads.size(); j++) {
				if (first) {
					code.append(loads.get(j)  + " BY properties#'tile'");
					first = false;
				} else
					code.append(", " + loads.get(j) + " BY properties#'tile'");
			}
			
			code.append(" PARALLEL " + _parallel + ";\n");
			_lastRelation = relation;
			
			relation = nextRelation("projection");
			code.append(relation + " = FOREACH " + _lastRelation + " GENERATE FLATTEN(II_Combine(");*/
			
			//String relation = nextRelation("union");
			code.append("union = UNION ");
			
			first = true;
			for (int i=0; i<loads.size(); i++) {
				if (first) {
					code.append(loads.get(i));
					first = false;
				} else
					code.append(", " + loads.get(i));
			}
		
			/*code.append(")) AS (geometry:bytearray, data:map[], properties:map[]);");
			_lastRelation = relation;*/
			
			code.append(";\n");
			
			//_lastRelation = relation;
			
			return;
			
		} else if (rule.getType().equals("Class")) {
			
			if (_firstClass) {				
				_firstClass = false;
				return;
			}
						
			//String name = rule.getLabel().trim();
			
			code.append("\n");
			
			//String relation = nextRelation("load");
			//code.append(relation + " = LOAD '' USING org.apache.pig.piggybank.storage.JsonLoader('geometry:bytearray, data:map[], properties:map[]');\n");
			//_lastRelation = relation;
			//_lastClassRelation = rule.getLabel();

			/*Initialization*/
			//String relation = nextRelation("selection");
			//code.append("selection = FILTER $LAST_RELATION BY (NOT II_IsEmpty(geometry)) AND II_IsValid(geometry);\n");
			//_lastRelation = relation;
			
			//relation = nextRelation("projection");
			//code.append(relation + " = FOREACH " + _lastRelation + " GENERATE geometry, data, II_ToProps('false','iirep',II_ToProps('false','iinrep',properties)) as properties;\n");
			//_lastRelation = relation;
			
			//relation = nextRelation("projection");
			//code.append(relation + " = FOREACH " + _lastRelation + " GENERATE II_CRSTransform(geometry, properties#'crs', 'EPSG:3857') AS geometry, data, properties;\n");			
			//_lastRelation = relation;
			
			//relation = nextRelation("projection");
			//code.append("projection = FOREACH $LAST_RELATION GENERATE geometry, data, II_ToProps(II_CalculateTiles(geometry),'tile',properties) AS properties;\n");
			//_lastRelation = relation;
			
			//relation = nextRelation("projection");
			//code.append(relation + " = FOREACH " + _lastRelation + " GENERATE FLATTEN(II_Replicate(geometry, data, properties)) AS (geometry:bytearray, data:map[], properties:map[]);\n");
			//_lastRelation = relation;
			
			//if (!rule.getParent().getType().equals("Union")) {
				loadSpectral(rule, code);
			//}
									
		} else if (rule.getType().equals("And")) {
			
			//String relation = null;
						
			if ((!rule.getParent().getType().equals("And")) && (!rule.getParent().getType().equals("Or"))) {
				//Computing the attribute
				//relation = nextRelation("selection");
				code.append("selection = FILTER $LAST_RELATION BY (");
			}
						
			boolean first = true;
			
			for (int k=0; k<rule.getChildren().size(); k++) {
				
				if ((!rule.getChildren().get(k).getType().equals("Or")) && (!rule.getChildren().get(k).getType().equals("And"))) {

					String fullExpression = rule.getChildren().get(k).getLabel();
					
					String[] expression = null;
					String op = null;
					String origOp = null;
					
					if (fullExpression.contains("=")) {
						op = "==";
						origOp = "=";
					} else if (fullExpression.contains("<")) {
						op = "<";
						origOp = "<";
					} else if (fullExpression.contains(">")) {
						op = ">";
						origOp = ">";
					} else if (fullExpression.contains("≤")) {
						op = "<=";
						origOp = "≤";
					} else if (fullExpression.contains("≥")) {
						op = ">=";
						origOp = "≥";
					} else if (fullExpression.contains("≠")) {
						op = "!=";
						origOp = "≠";
					}
										
					if (origOp == null)
						return;
					
					expression = fullExpression.split(origOp);
					
					if (expression.length<2)
						return;
					
					expression[0] = expression[0].trim();
					expression[1] = expression[1].trim();
					
					if (first) {
						code.append("(" + parseExpression(expression[0]) + " " + op + " " + parseExpression(expression[1]) + ")");
						first = false;
					} else
						code.append(" AND (" + parseExpression(expression[0]) + " " + op + " " + parseExpression(expression[1]) + ")");
										
				} else {
					code.append(" AND (");
					evaluateRule(rule.getChildren().get(k), code);
					code.append(")");
				}
												
			}
			
			if ((!rule.getParent().getType().equals("And")) && (!rule.getParent().getType().equals("Or"))) {
				code.append(");\n");
				//_lastRelation = relation;
			}
						
			return;
			
		} else if (rule.getType().equals("Or")) {
			
			//String relation = null;
			
			if ((!rule.getParent().getType().equals("And")) && (!rule.getParent().getType().equals("Or"))) {			
				//Computing the attribute
				//relation = nextRelation("selection");
				code.append("selection = FILTER $LAST_RELATION BY (");			
			}
			
			boolean first = true;
			
			for (int k=0; k<rule.getChildren().size(); k++) {
				
				if ((!rule.getChildren().get(k).getType().equals("Or")) && (!rule.getChildren().get(k).getType().equals("And"))) {
					
					String fullExpression = rule.getChildren().get(k).getLabel();
					
					String[] expression = null;
					String op = null;
					String origOp = null;
					
					if (fullExpression.contains("=")) {
						op = "==";
						origOp = "=";
					} else if (fullExpression.contains("<")) {
						op = "<";
						origOp = "<";
					} else if (fullExpression.contains(">")) {
						op = ">";
						origOp = ">";
					} else if (fullExpression.contains("≤")) {
						op = "<=";
						origOp = "≤";
					} else if (fullExpression.contains("≥")) {
						op = ">=";
						origOp = "≥";
					} else if (fullExpression.contains("≠")) {
						op = "!=";
						origOp = "≠";
					}
										
					if (origOp == null)
						return;
					
					expression = fullExpression.split(origOp);
					
					if (expression.length<2)
						return;
					
					expression[0] = expression[0].trim();
					expression[1] = expression[1].trim();
					
					if (first) {
						code.append("(" + parseExpression(expression[0]) + " " + op + " " + parseExpression(expression[1]) + ")");
						first = false;
					} else
						code.append(" OR (" + parseExpression(expression[0]) + " " + op + " " + parseExpression(expression[1]) + ")");
										
				} else {
					code.append(" OR (");
					evaluateRule(rule.getChildren().get(k), code);
					code.append(")");
				}
												
			}
			
			if ((!rule.getParent().getType().equals("And")) && (!rule.getParent().getType().equals("Or"))) {
				code.append(");\n");
				//_lastRelation = relation;				
			}
						
			return;
			
		} else if (rule.getType().equals("Logic")) {
			
			if ((rule.getParent().getType().equals("And")) || (rule.getParent().getType().equals("Or")))
				return;
			else {
				
				//String relation;
				
				String fullExpression = rule.getLabel();
				
				String[] expression = null;
				String op = null;
				String origOp = null;
								
				if (fullExpression.contains("=")) {
					op = "==";
					origOp = "=";
				} else if (fullExpression.contains("<")) {
					op = "<";
					origOp = "<";
				} else if (fullExpression.contains(">")) {
					op = ">";
					origOp = ">";
				} else if (fullExpression.contains("≤")) {
					op = "<=";
					origOp = "≤";
				} else if (fullExpression.contains("≥")) {
					op = ">=";
					origOp = "≥";
				} else if (fullExpression.contains("≠")) {
					op = "!=";
					origOp = "≠";
				}
								
				expression = fullExpression.split(origOp);
				
				if (expression.length<2)
					return;
				
				expression[0] = expression[0].trim();
				expression[1] = expression[1].trim();
				
				//Computing the attribute
				//relation = nextRelation("selection");
				code.append("selection = FILTER $LAST_RELATION BY " + parseExpression(expression[0]) + " " + op + " " + parseExpression(expression[1]) + ";\n");
				//_lastRelation = relation;				
								
			}
			
		} else if (rule.getType().equals("Fuzzy")) {
			
			List<String> aggregations = new ArrayList<String>();
			aggregations.add("Min");
			aggregations.add("Max");
			aggregations.add("Mul");
			aggregations.add("Sum");
			aggregations.add("Mean");
						
			if ((rule.getParent().getType().equals("Fuzzy")) && (aggregations.contains(rule.getParent().getLabel().trim()))) {
								
				//Nested fuzzy
				if ((rule.getType().equals("Fuzzy")) && (aggregations.contains(rule.getLabel().trim()))) {
				
					boolean first = true;

					String aggregation = rule.getLabel();
					
					code.append(" II_" + aggregation + "(");
					
					for (int k=0; k<rule.getChildren().size(); k++) {
						
						if (!aggregations.contains(rule.getChildren().get(k).getLabel().trim())) {
							
							String fullExpression = rule.getChildren().get(k).getLabel();
							
							if (fullExpression.contains("(")) {
								
								String[] expression = fullExpression.split("\\(");
								
								expression[0] = expression[0].trim();
								expression[1] = expression[1].trim();
								
								//Removing last parenthesis
								expression[1] = expression[1].substring(0,expression[1].length()-1);
							
								if (first) {
									code.append("II_Membership('" + expression[0] + "'," + parseExpression(expression[1]) + ")");
									first = false;
								} else {
									code.append(",II_Membership('" + expression[0] + "'," + parseExpression(expression[1]) + ")");
								}	
								
							} else {
			
								fullExpression = fullExpression.trim();
								if (first) {																	
									code.append(fullExpression);
									first = false;
								} else {
									code.append("," + fullExpression);
								}
							}
														
						} else {
							
							if (first) {
								evaluateRule(rule.getChildren().get(k), code);
								first = false;
							} else {
								code.append(",");
								evaluateRule(rule.getChildren().get(k), code);
							}
							
						}
						
					}
					
					code.append(")");
					
					return;
					
				} else {
					/*The parent will handle normal fuzzy rules*/
					return;
				}
				
			} else {
			
				if ((rule.getType().equals("Fuzzy")) && (aggregations.contains(rule.getLabel().trim()))) {
					
					//Computing the attribute
					//String relation = nextRelation("projection");
					code.append("projection = FOREACH $LAST_RELATION GENERATE *,");
					
					boolean first = true;
					
					String aggregation = rule.getLabel();
					
					code.append(" II_" + aggregation + "(");
					
					for (int k=0; k<rule.getChildren().size(); k++) {
						
						if (!aggregations.contains(rule.getChildren().get(k).getLabel().trim())) {
														
							String fullExpression = rule.getChildren().get(k).getLabel();
							
							if (fullExpression.contains("(")) {
								
								String[] expression = fullExpression.split("\\(");
								
								expression[0] = expression[0].trim();
								expression[1] = expression[1].trim();
								
								//Removing last parenthesis
								expression[1] = expression[1].substring(0,expression[1].length()-1);
							
								if (first) {
									code.append("II_Membership('" + expression[0] + "'," + parseExpression(expression[1]) + ")");
									first = false;
								} else {
									code.append(",II_Membership('" + expression[0] + "'," + parseExpression(expression[1]) + ")");
								}	
								
							} else {
			
								fullExpression = fullExpression.trim();
								if (first) {																	
									code.append(fullExpression);
									first = false;
								} else {
									code.append("," + fullExpression);
								}
								
							}
														
						} else {
							
							if (first) {
								evaluateRule(rule.getChildren().get(k), code);
								first = false;
							} else {
								code.append(",");
								evaluateRule(rule.getChildren().get(k), code);
							}
							
						}
												
					}
					
					code.append(") AS membership;\n");
					
					//Creating the new property
					//relation = nextRelation("projection");
					code.append("projection = FOREACH $LAST_RELATION GENERATE geometry, data, II_ToClassification('$CLASS', membership, properties) AS properties;\n");			
					//_lastRelation = relation;
					
					return;
					
				} else {
				
					//String relation;
					
					String fullExpression = rule.getLabel();
					
					if (fullExpression.contains("(")) {
						
						String[] expression = fullExpression.split("\\(");
						
						expression[0] = expression[0].trim();
						expression[1] = expression[1].trim();
						
						//Removing last parenthesis
						expression[1] = expression[1].substring(0,expression[1].length()-1);
					
						//Computing the attribute
						//relation = nextRelation("projection");
						code.append("projection = FOREACH $LAST_RELATION GENERATE *, II_Membership('" + expression[0] + "'," + parseExpression(expression[1]) + ") AS membership;\n");
						//_lastRelation = relation;
									
						//Creating the new property
						//relation = nextRelation("projection");
						code.append("projection = FOREACH $LAST_RELATION GENERATE geometry, data, II_ToClassification('$CLASS', membership, properties) AS properties;\n");			
						//_lastRelation = relation;	
						
					} else {
	
						fullExpression = fullExpression.trim();
						
						//Creating the new property
						//relation = nextRelation("projection");
						code.append("projection = FOREACH $LAST_RELATION GENERATE geometry, data, II_ToClassification('$CLASS', " + fullExpression + ", properties) AS properties;\n");			
						//_lastRelation = relation;
						
					}
					
				}
				
			}
			
		} else if (rule.getType().equals("Expression")) {
			
			//String relation;
			
			String fullExpression = rule.getLabel();
			
			String[] expression = fullExpression.split("=");
						
			if (expression.length<2)
				return;
			
			expression[0] = expression[0].trim();
			expression[1] = expression[1].trim();
			
			String[] tokens = expression[1].split("\\(");
						
			if (_udfSet.getUDFs().containsKey(tokens[0])) {
				if (_udfSet.getUDFs().get(tokens[0]).get("isSpectral").equals("true")) {
					return;
				}
			}
			
			//Computing the attribute
			//relation = nextRelation("projection");
			code.append("projection = FOREACH $LAST_RELATION GENERATE *, " + parseExpression(expression[1]) + " AS " + expression[0] + ";\n");
			//_lastRelation = relation;			
						
			//Creating the new property
			//relation = nextRelation("projection");
			code.append("projection = FOREACH $LAST_RELATION GENERATE geometry, data, II_ToProps(" + expression[0] + ", '" + expression[0] + "', properties) AS properties;\n");			
			//_lastRelation = relation;
					
		}
		
		for (int k=0; k<rule.getChildren().size(); k++) {
			evaluateRule(rule.getChildren().get(k), code);
		}
		
	}
	
	private void checkSpectral(Rule rule) {
		
		String fullExpression = rule.getLabel();
				
		if (rule.getType().equals("Expression")) {
		
			String[] expression = fullExpression.split("=");
		
			if (expression.length==2) {
			
				expression[0] = expression[0].trim();
				expression[1] = expression[1].trim();
				
				String[] tokens = expression[1].split("\\(");
							
				if (_udfSet.getUDFs().containsKey(tokens[0])) {
					if (_udfSet.getUDFs().get(tokens[0]).get("isSpectral").equals("true")) {
						_spectralCalculations.add(fullExpression);
					}
				}
				
			}
			
		}
			
		
		for (int k=0; k<rule.getChildren().size(); k++) {		
			checkSpectral(rule.getChildren().get(k));		
		}

	}
	
	public String getPigCode() {
		
		StringBuilder code = new StringBuilder();

		/*Pig setup*/
		
		/*try {
		
			Properties pig = new Properties();
			
			InputStream pigInput = new FileInputStream("pig.properties");
			
			pig.load(pigInput);
			
			for (Map.Entry<Object, Object> entry : pig.entrySet()) {
				String key = (String)entry.getKey();
				String value = (String)entry.getValue();
				code.append("SET " + key + " " + value + "\n");
			}
			
		} catch (Exception e) {
			System.err.println("Could not read PIG properties file: " + e.getMessage());
		}*/
				
		//code.append("\n");
		
		/*Including JARs*/
		
		/*File folder = new File("lib");
		
		for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	        	//ignore
	        } else {
	        	code.append("REGISTER " + _properties.getProperty("interimage.sourceURL") + "libs/" + fileEntry.getName() + ";\n");
	        }
	    }*/
	
		/*Defines*/		
		/*try {
		
			// Stream for output file 
			OutputStream out = new FileOutputStream("interimage-import.pig");
			
			for (Map.Entry<String, Map<String, Object>> entry : _udfSet.getUDFs().entrySet()) {
				//String name = entry.getKey();
				Map<String, Object> map = entry.getValue();
				
				String str = null;
				
				if (!map.get("lazyDefinition").equals("true")) {
				
					str = "DEFINE " + map.get("alias") + " " + map.get("import");
					
					@SuppressWarnings("unchecked")
					List<String> params = (List<String>)map.get("params");
					
					if (params.size()>0) {
					
						str = str + "(";
						
						boolean first = true;
						for (String p : params) {
							
							String property = p;
							
							//if (_properties.containsKey("interimage." + name + "." + p)) {
							//	property = _properties.getProperty("interimage." + name + "." + p);
							//} else if (_properties.containsKey("interimage." + p)) {
							//	property = _properties.getProperty("interimage." + p);
							//} else {
							//	property = "";
							//}
							
							if (first) {
								str = str + "'" + property + "'";
								first = false;
							} else
								str = str + ",'" + property + "'";
						}
						
						str = str + ")";
						
					}
					
					str = str + ";\n";
					
				} else {
					str = "";
				}
				
				@SuppressWarnings("unchecked")
				List<String> macros = (List<String>)map.get("macros");

				for (String m : macros) {
					str = str + m;
				}
				
				out.write(str.getBytes());
			}
			
			out.close();
			
		} catch (Exception e) {
			System.err.println("Could not create PIG import file: " + e.getMessage());
		}*/
		
		//code.append("IMPORT '" + _properties.getProperty("interimage.sourceURL") + "resources/scripts/interimage-import.pig';\n");
		
		code.append("\n");
		
		evaluateRule(_root, code);
		
		//TODO: Implement merging, No Merge is implemented by omission
		//Decided to by pass this configuration as it seems that the translator of the
		//semantic network should decide whether to merge or not
		/*if (_root.getLabel().equals("Merge All")) {
			
		} else if (_root.getLabel().equals("Merge Connected")) {
			
		} else if (_root.getLabel().equals("No Merge")) {
			
		}*/
			
		code.append("BEGIN IF $OUTPUT.ROI\n");
		code.append("INCLUDE ExportROI\n");
		code.append("END IF $OUTPUT.ROI\n");
		
		return code.toString();
	}
	
	private int countRules(Rule rule) {
		int count = rule.getChildren().size();
		for (int k=0; k<rule.getChildren().size(); k++) {			
			count = count + countRules(rule.getChildren().get(k));
		}
		return count;
	}

	public int size() {
		return countRules(_root) + 1;		
	}
	
	/*public Map<String,Integer> getCounts() {
		return _counts;
	}*/
	
	public String getURL() {
		return _url;
	}
	
}
