/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Grouping comparator used for the source-to-target pattern
 *         translation feature. Rules are compared by their source pattern.
 */
public class TargetPatternGroupingComparator extends WritableComparator {

    protected TargetPatternGroupingComparator() {
        super(RuleWritable.class);
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.
     * WritableComparable, org.apache.hadoop.io.WritableComparable)
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        RuleWritable targetPatternA = ((RuleWritable) a).getTargetPattern();
        RuleWritable targetPatternB = ((RuleWritable) b).getTargetPattern();
        return targetPatternA.compareTo(targetPatternB);
    }
}
