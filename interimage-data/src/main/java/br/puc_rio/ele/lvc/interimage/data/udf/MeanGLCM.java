package br.puc_rio.ele.lvc.interimage.data.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.data.DataParser;
import br.puc_rio.ele.lvc.interimage.data.GLCMtexture;
import ij.process.ImageProcessor;
import ij.ImagePlus;


/**
 * A UDF that returns the mean GLCM of a raster layer given an angle.
 * Example:
 * 		A = load 'mydata' as (raster);
 * 		B = foreach A generate MeanGLCM(raster#'0',190');
 * @author Patrick Happ
 *
 */
public class MeanGLCM extends EvalFunc<Double> {
	
	private final DataParser imageParser = new DataParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; first column is assumed to have the raster layer
     * @param angle;
     * @exception java.io.IOException
     * @return GLCM mean, or null in case of processing error
     */
	@Override
	public Double exec(Tuple input) throws IOException {		
		if (input == null || input.size() < 2)
            return null;
        
		try {			
			Object objImage = input.get(0);
			ImageProcessor ip = imageParser.parseImgProc(objImage);
			ImagePlus imp = new ImagePlus("", ip);
			
			String strIndex = (String)input.get(1);
			int angle = Integer.parseInt(strIndex);

			if (!(angle == 0 || angle ==45 || angle ==90 || angle == 135 || angle ==180 || angle ==225 || angle ==270 || angle ==315))
				System.out.println("Angle is out of possible range!");
			
			GLCMtexture glcm = new GLCMtexture(imp, angle, true, false);
			ImageProcessor ip8 = ip.convertToByte(false);
			glcm.calcGLCM(ip8);
			glcm.doBasicStats();
		
			return glcm.getGLCMMean();

		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
	
}