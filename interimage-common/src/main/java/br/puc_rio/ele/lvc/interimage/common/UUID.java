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

package br.puc_rio.ele.lvc.interimage.common;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Help class to compute UUIDs. The available types are MD5, SHA-1 and SHA-256.
 * @author Rodrigo Ferreira
 */
public class UUID {

	private String _type;
	
	public UUID(String type) {
		_type = type;
	}
	
	/**
     * Method that creates a hash based on a string.
     * @param input string
     * @return hash string
     */
	public String digest(String string) {
		
		MessageDigest md = null;
	    String uuid = null;
	    
	    try {
            md = MessageDigest.getInstance(_type);  
        } catch (NoSuchAlgorithmException e) {  
            e.printStackTrace();  
        }
	    
	    try {
		    BigInteger hash = new BigInteger(1, md.digest(string.getBytes("UTF-8")));  
	    	uuid = hash.toString(16);

	    } catch (UnsupportedEncodingException e) {	    	     
	    	e.printStackTrace();  
	    }
	    
        return uuid;
		
	}
	
	/**
     * Method that creates a hash based on an array of bytes.
     * @param array of bytes
     * @return hash string
     */
	public String digest(byte[] bytes) {
		
		MessageDigest md = null;
	    
	    try {
            md = MessageDigest.getInstance(_type);  
        } catch (NoSuchAlgorithmException e) {  
            e.printStackTrace();  
        }
	    
	    String uuid = null;
	    
	    BigInteger hash = new BigInteger(1, md.digest(bytes));
    	uuid = hash.toString(16);
        	    
        return uuid;
		
	}
	
	/**
     * Method that creates a random hash.
     * @return hash string
     */
	public String random() {
		java.util.UUID uuid = java.util.UUID.randomUUID();
		return uuid.toString();
	}
	
}
