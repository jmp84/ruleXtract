/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84
 */
public class UnalignedSourceWords implements Feature {

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public double value(Rule r, ArrayWritable mapReduceFeatures) {
        // TODO check length
        return ((DoubleWritable) mapReduceFeatures.get()[3]).get();
    }

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#valueAsciiOovDeletion(uk.
     * ac.cam.eng.extraction.datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public double
            valueAsciiOovDeletion(Rule r, ArrayWritable mapReduceFeatures) {
        return 0;
    }

    /*
     * (non-Javadoc)
     * @see uk.ac.cam.eng.rulebuilding.features.Feature#valueGlue(uk.ac.cam.eng.
     * extraction.datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public double valueGlue(Rule r, ArrayWritable mapReduceFeatures) {
        return 0;
    }

}
