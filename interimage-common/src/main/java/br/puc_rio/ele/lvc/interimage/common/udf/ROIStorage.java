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

package br.puc_rio.ele.lvc.interimage.common.udf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.iq80.snappy.SnappyOutputStream;

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * A UDF that stores a ROI shape.<br><br>
 * Example:<br>
 * 		A = load 'mydata';<br>
 * 		B = foreach A generate ROIStorage(geometry, classname, shapes_key);
 * @author Rodrigo Ferreira
 *
 */
public class ROIStorage extends EvalFunc<Boolean> {
	
	private final GeometryParser _geometryParser = new GeometryParser();
	
	private String _accessKey;
	private String _secretKey;
	private String _bucket;
	
	public ROIStorage(String accessKey, String secretKey, String bucket) {
		_accessKey = accessKey;
		_secretKey = secretKey;
		_bucket = bucket;
	}
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have the geometry<br>
     * second column is assumed to have the class name<br>
     * third column is assumed to have the output path
     * @exception java.io.IOException
     * @return true if successful, false otherwise
     */
	@Override
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() < 3)
            return null;
        
		try {
			
			Object objGeometry = input.get(0);
			Geometry geometry = _geometryParser.parseGeometry(objGeometry);			
			String className = DataType.toString(input.get(1));
			String path = DataType.toString(input.get(2));
			
			AWSCredentials credentials = new BasicAWSCredentials(_accessKey, _secretKey);
			AmazonS3 conn = new AmazonS3Client(credentials);
			conn.setEndpoint("https://s3.amazonaws.com");
			
			/*File temp = File.createTempFile(className, ".wkt");
			
		    // Delete temp file when program exits.
		    temp.deleteOnExit();
							
		    BufferedWriter out = new BufferedWriter(new FileWriter(temp));
		    out.write(new WKTWriter().write(geometry));
		    out.close();*/
		    
			/*
			
			File temp = File.createTempFile(className, ".wkt.snappy");
				
			temp.deleteOnExit();*/
			
			String geom = new WKTWriter().write(geometry);
						
		    ByteArrayOutputStream out = new ByteArrayOutputStream();

		    OutputStream snappyOut  = new SnappyOutputStream(out);
		    snappyOut.write(geom.getBytes());
		    snappyOut.close();
			
			/*PutObjectRequest putObjectRequest = new PutObjectRequest(_bucket, path + className + ".wkt.snappy", temp);
			putObjectRequest.withCannedAcl(CannedAccessControlList.PublicRead); // public for all*/
						
			PutObjectRequest putObjectRequest = new PutObjectRequest(_bucket, path + className + ".wkts", new ByteArrayInputStream(out.toByteArray()), new ObjectMetadata());
			putObjectRequest.withCannedAcl(CannedAccessControlList.PublicRead); // public for all
			
			TransferManager tx = new TransferManager(credentials);
			tx.upload(putObjectRequest);
			
			return true;
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
    }
	
}
