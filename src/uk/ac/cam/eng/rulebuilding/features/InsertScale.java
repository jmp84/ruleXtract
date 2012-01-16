/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import org.apache.hadoop.io.ArrayWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84
 */
public class InsertScale implements Feature {

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
     */
    @Override
    public double value(Rule r, ArrayWritable mapReduceFeatures) {
        // TODO make this work for deletion and oov rules
        return (int) 0;
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
        // deletion rule
        if (r.getTargetWords().size() == 1 && r.getTargetWords().get(0) == 0) {
            return -1;
        }
        // oov rule
        if (r.getTargetWords().size() == 0) {
            return -1;
        }
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
