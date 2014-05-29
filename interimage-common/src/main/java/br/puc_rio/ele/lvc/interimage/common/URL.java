package br.puc_rio.ele.lvc.interimage.common;

import java.io.File;

public class URL {

	public static boolean isLocalFile(String file) {
    	if ((file.startsWith("http://")) || (file.startsWith("https://")))
    		return false;
    	
    	return true;
	}
	
	public static String getPath(String file) {
		
		int idx = file.lastIndexOf(File.separatorChar);
        String path = file.substring(0, idx + 1);
        
        return path;
		
	}
	
	public static String getFileName(String file) {
		
		int idx = file.lastIndexOf(File.separatorChar);
        String fileName = file.substring(idx + 1);

        idx = fileName.lastIndexOf(".");

        if (idx == -1) {
        	return null;
        }
        
        return fileName;
		
	}
	
	public static String getFileNameWithoutExtension(String file) {
		
		int idx = file.lastIndexOf(File.separatorChar);
        String fileName = file.substring(idx + 1);

        idx = fileName.lastIndexOf(".");

        if (idx == -1) {
        	return null;
        }
        
        String fileNameWithoutExtention = fileName.substring(0, idx);
        
        return fileNameWithoutExtention;
		
	}
	
}
