/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import org.apache.hadoop.io.ArrayWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84 This class represents the reorder scale feature. I am not sure
 *         what this feature is doing, but it is essentially a binary feature
 *         that is activated when we use a start of sentence rule
 *         (X-->1,<s>_<s>_<s>) or a glue rule (X-->V V) In the original
 *         implementation, the value is not 1, it is -0.01005033585350145059
 *         Update: this feature is actually useless now
 */
public class ReorderScale implements Feature {

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .Rule)
     */
    @Override
    public double value(Rule r, ArrayWritable mapReduceFeatures) {
        return (int) (r.isStartingGlue() ? 1 : 0);
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
