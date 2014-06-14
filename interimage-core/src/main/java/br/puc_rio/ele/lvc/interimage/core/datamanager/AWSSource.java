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

package br.puc_rio.ele.lvc.interimage.core.datamanager;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;

/**
 * A Source class that communicates with Amazon S3. 
 * @author Rodrigo Ferreira
 */
@SuppressWarnings("unused")
public class AWSSource implements Source {

	private String _accessKey;	
	private String _secretKey;
	private String _bucket;
	
	public AWSSource(String accessKey, String secretKey, String bucket) {
		_accessKey = accessKey;
		_secretKey = secretKey;
		_bucket = bucket;
	}
	
	public void put(String from, String to) {

		try {
		
			AWSCredentials credentials = new BasicAWSCredentials(_accessKey, _secretKey);
			AmazonS3 conn = new AmazonS3Client(credentials);
			conn.setEndpoint("https://s3.amazonaws.com");
					
			FileInputStream stream = new FileInputStream(from);
						
			conn.putObject(_bucket, to, stream, new ObjectMetadata());
			
		} catch (Exception e) {
			System.err.println("Source put failed: " + e.getMessage());			
		}
		
	}
	
}
