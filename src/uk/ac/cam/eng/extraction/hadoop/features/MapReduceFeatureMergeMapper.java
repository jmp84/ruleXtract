/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Mapper;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable2;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.util.Util;

/**
 * @author jmp84
 */
public class MapReduceFeatureMergeMapper
        extends
        Mapper<RuleWritable, MapWritable, BytesWritable, GeneralPairWritable2> {

    private static RuleWritable source = new RuleWritable();
    private static BytesWritable sourceBytesWritable = new BytesWritable();
    private static RuleWritable target = new RuleWritable();
    private static GeneralPairWritable2 targetAndFeatures =
            new GeneralPairWritable2();

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(RuleWritable key, MapWritable value,
            Context context)
            throws IOException, InterruptedException {
        source.setSource(key.getSource());
        target.setTarget(key.getTarget());
        targetAndFeatures.setFirst(target);
        targetAndFeatures.setSecond(value);
        byte[] sourceByteArray = Util.object2ByteArray(source);
        sourceBytesWritable.set(sourceByteArray, 0, sourceByteArray.length);
        context.write(sourceBytesWritable, targetAndFeatures);
    }
}
