/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RulePatternWritable;
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
        if (a.getClass() == RulePatternWritable.class
                && b.getClass() == RuleWritable.class) {
            return -1;
        }
        if (a.getClass() == RuleWritable.class
                && b.getClass() == RulePatternWritable.class) {
            return 1;
        }
        if (a.getClass() == RulePatternWritable.class
                && b.getClass() == RulePatternWritable.class) {
            RulePatternWritable patternA = (RulePatternWritable) a;
            RulePatternWritable patternB = (RulePatternWritable) b;
            if (patternA.isTargetEmpty() && !patternB.isTargetEmpty()) {
                return -1;
            }
            if (!patternA.isTargetEmpty() && patternB.isTargetEmpty()) {
                return 1;
            }
            return patternA.compareTo(patternB);
        }
        RulePatternWritable patternA =
                new RulePatternWritable((RuleWritable) a);
        RulePatternWritable targetPatternA = patternA.makeTargetMarginal();
        RulePatternWritable patternB =
                new RulePatternWritable((RuleWritable) b);
        RulePatternWritable targetPatternB = patternB.makeTargetMarginal();
        return targetPatternA.compareTo(targetPatternB);
    }
}
