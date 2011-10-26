/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84 This class represents the glue rule feature, that is the number
 *         of times the rule S-->SX,SX is found
 */
public class GlueRule implements Feature {

    /*
     * (non-Javadoc)
     * @see
     * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
     * .Rule)
     */
    @Override
    public double value(Rule r) {
        return r.isConcatenatingGlue() ? 1 : 0;
    }

}
