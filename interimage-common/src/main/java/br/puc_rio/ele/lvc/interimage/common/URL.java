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

import java.net.HttpURLConnection;

/**
 * Help class to work with URLs.
 * @author Rodrigo Ferreira
 */
public class URL {

	public static boolean isLocalFile(String file) {
    	if ((file.startsWith("http://")) || (file.startsWith("https://")))
    		return false;
    	
    	return true;
	}
	
	public static String getPath(String file) {
		
		int idx = file.lastIndexOf("/");
		
		if (idx==-1)
			idx = file.lastIndexOf("\\");
		
        String path = file.substring(0, idx + 1);
        
        return path;
		
	}
	
	public static String getFileName(String file) {
		
		int idx = file.lastIndexOf("/");
		
		if (idx==-1)
			idx = file.lastIndexOf("\\");
		
        String fileName = file.substring(idx + 1);

        idx = fileName.lastIndexOf(".");

        if (idx == -1) {
        	return null;
        }
        
        return fileName;
		
	}
	
	public static String getFileNameWithoutExtension(String file) {
		
		int idx = file.lastIndexOf("/");
		
		if (idx==-1)
			idx = file.lastIndexOf("\\");
		
        String fileName = file.substring(idx + 1);

        idx = fileName.lastIndexOf(".");

        if (idx == -1) {
        	return null;
        }
        
        String fileNameWithoutExtention = fileName.substring(0, idx);
        
        return fileNameWithoutExtention;
		
	}
	
	public static boolean exists(String URLName){
	    try {
	      HttpURLConnection.setFollowRedirects(false);
	      // note : you may also need
	      //        HttpURLConnection.setInstanceFollowRedirects(false)
	      HttpURLConnection con =
	         (HttpURLConnection) new java.net.URL(URLName).openConnection();
	      con.setRequestMethod("HEAD");
	      return (con.getResponseCode() == HttpURLConnection.HTTP_OK);
	    }
	    catch (Exception e) {
	       e.printStackTrace();
	       return false;
	    }
	  }
	
}
