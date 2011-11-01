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
public class RuleCountGreaterThan2 implements Feature {

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public double value(Rule r, ArrayWritable mapReduceFeatures) {
        double count = ((DoubleWritable) mapReduceFeatures.get()[2]).get();
        return (count > 2) ? 1 : 0;
    }

}
