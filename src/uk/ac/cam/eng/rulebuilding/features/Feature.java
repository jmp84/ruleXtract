/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84 This interface represents a feature that can be computed on the
 *         fly, for example the word insertion penalty
 */
public interface Feature {

    public double value(Rule r);

}
