/**
 * 
 */
package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import uk.ac.cam.eng.extraction.RuleExtractor;
import uk.ac.cam.eng.extraction.datatypes.Alignment;
import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.datatypes.SentencePair;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;

/**
 * @author juan
 * This class is a mapper that that takes a target rule as key
 * and a list of sources along with features as value and converts
 * them into a list of pairs (key, value) where the key is a source
 * and the value is a target along with features. This mapper is used
 * in a mapreduce job that converts the output of a target-to-source
 * extraction job into an output similar to the output of a source-to-target
 * extraction job, that is sorted by source.
 */
public class Target2Source2Source2TargetMapper extends
Mapper<BytesWritable, ArrayWritable, BytesWritable, PairWritable3> {

	// TODO avoid duplicating this method (already in RuleFileBuilder)
    private static ArrayWritable convertValueBytes(ByteBuffer bytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes.array(), bytes.arrayOffset(), bytes.limit());
        ArrayWritable value = new ArrayWritable(PairWritable3.class);
        try {
            value.readFields(in);
        }
        catch (IOException e) {
            // Byte buffer is memory backed so no exception is possible. Just in
            // case chain it to a runtime exception
            throw new RuntimeException(e);
        }
        return value;
    }
  
    // TODO make this method general (inheritance maybe, see Rory's example)
    private static RuleWritable convertValueBytes2RuleWritable(byte[] bytes) {
        DataInputBuffer in = new DataInputBuffer();
        //in.reset(bytes, bytes.arrayOffset(), bytes.limit());
        in.reset(bytes, bytes.length);
        RuleWritable value = new RuleWritable();
        try {
            value.readFields(in);
        }
        catch (IOException e) {
            // Byte buffer is memory backed so no exception is possible. Just in
            // case chain it to a runtime exception
            throw new RuntimeException(e);
        }
        return value;
    }
    
 // TODO avoid duplicating this method (already in ConvertToBytesReducer)
    private byte[] object2ByteArray(Writable obj) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        obj.write(out);
        return buffer.toByteArray();
    }
	
	/**                                                                                                                                                                                                    
	 *                                                                                                                                  
	 */
	@Override
	protected void
	map(BytesWritable key, ArrayWritable value, Context context)
			throws java.io.IOException, InterruptedException {
		PairWritable3[] sourcesAndFeatures = (PairWritable3[])value.get();
		for (PairWritable3 sourceAndFeatures: sourcesAndFeatures) {
			RuleWritable source = convertValueBytes2RuleWritable(key.getBytes());
			PairWritable3 targetAndFeatures = new PairWritable3(source, sourceAndFeatures.second);
			BytesWritable outputKey = new BytesWritable(object2ByteArray(sourceAndFeatures.first));
			context.write(outputKey, targetAndFeatures);
		}
	}
}