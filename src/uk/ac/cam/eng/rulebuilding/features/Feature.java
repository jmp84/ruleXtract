/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import java.util.List;

import org.apache.hadoop.io.ArrayWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84 This interface represents a feature that can be computed on the
 *         fly, for example the word insertion penalty
 */
public interface Feature {

    public List<Double> value(Rule r, ArrayWritable mapReduceFeatures);

    public List<Double>
            valueAsciiOovDeletion(Rule r, ArrayWritable mapReduceFeatures);

    public List<Double> valueGlue(Rule r, ArrayWritable mapReduceFeatures);
    
    public int getNumberOfFeatures();

}
