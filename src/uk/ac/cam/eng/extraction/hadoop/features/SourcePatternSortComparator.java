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
public class SourcePatternSortComparator extends WritableComparator {

    protected SourcePatternSortComparator() {
        // create instances, otherwise null pointer exception
        super(RuleWritable.class, true);
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
            if (ra.isTargetEmpty() && !rb.isTargetEmpty()) {
                return -1;
            }
            if (!ra.isTargetEmpty() && ra.isTargetEmpty()) {
                return 1;
            }
            return ra.compareTo(rb);
        }
        RuleWritable sourcePatternA = ra.getSourcePattern();
        RuleWritable sourcePatternB = rb.getSourcePattern();
        return sourcePatternA.compareTo(sourcePatternB);
    }
}
