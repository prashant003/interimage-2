package br.puc_rio.ele.lvc.interimage.common;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Help class to compute UUIDs. The available types are MD5, SHA-1 and SHA-256.
 * @author Rodrigo Ferreira
 *
 */
public class UUID {

	private String _type;
	
	public UUID(String type) {
		_type = type;
	}
	
	public String digest(String string) {
		
		MessageDigest md = null;
	    
	    try {
            md = MessageDigest.getInstance(_type);  
        } catch (NoSuchAlgorithmException e) {  
            e.printStackTrace();  
        }
	    
	    String uuid = null;
	    
	    try {
		    BigInteger hash = new BigInteger(1, md.digest(string.getBytes("UTF-8")));  
	    	uuid = hash.toString(16);

	    } catch (UnsupportedEncodingException e) {	    	     
	    	e.printStackTrace();  
	    }
	    
        return uuid;
		
	}
	
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
	
	public String random() {
		java.util.UUID uuid = java.util.UUID.randomUUID();
		return uuid.toString();
	}
	
}
