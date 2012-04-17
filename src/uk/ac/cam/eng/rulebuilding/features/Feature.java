/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.features;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SortedMapWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84 This interface represents a feature that can be computed on the
 *         fly, for example the word insertion penalty
 */
public interface Feature {

    // TODO have two abstract classes implementing Feature. one for mr features
    // one for features

    public Map<Integer, Number> value(Rule r,
            SortedMapWritable mapReduceFeatures, Configuration conf);

    public Map<Integer, Number>
            valueAsciiOovDeletion(Rule r, SortedMapWritable mapReduceFeatures,
                    Configuration conf);

    public Map<Integer, Number> valueGlue(Rule r,
            SortedMapWritable mapReduceFeatures, Configuration conf);

    public int getNumberOfFeatures(Configuration conf);

}
