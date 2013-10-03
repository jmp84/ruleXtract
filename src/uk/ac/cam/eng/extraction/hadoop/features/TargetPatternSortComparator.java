/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Sort comparator used for the source-to-target pattern
 *         probability feature. Rules are compared by their source pattern.
 */
public class TargetPatternSortComparator extends WritableComparator {

    protected TargetPatternSortComparator() {
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
        RuleWritable ra = (RuleWritable) a;
        RuleWritable rb = (RuleWritable) b;
        if (ra.isPattern() && !rb.isPattern()) {
            return -1;
        }
        if (!ra.isPattern() && rb.isPattern()) {
            return 1;
        }
        if (ra.isPattern() && rb.isPattern()) {
            if (ra.isSourceEmpty() && !rb.isSourceEmpty()) {
                return -1;
            }
            if (!ra.isSourceEmpty() && ra.isSourceEmpty()) {
                return 1;
            }
            return ra.compareTo(rb);
        }
        RuleWritable targetPatternA = ra.getTargetPattern();
        RuleWritable targetPatternB = rb.getTargetPattern();
        return targetPatternA.compareTo(targetPatternB);
    }
}
