/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RulePatternWritable;
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
        RulePatternWritable targetPatternA = null;
        if (a.getClass() == RuleWritable.class) {
            RulePatternWritable patternA =
                    new RulePatternWritable((RuleWritable) a);
            targetPatternA = patternA.makeTargetMarginal();
        }
        else if (a.getClass() == RulePatternWritable.class) {
            targetPatternA = (RulePatternWritable) a;
        }
        RulePatternWritable targetPatternB = null;
        if (b.getClass() == RuleWritable.class) {
            RulePatternWritable patternB =
                    new RulePatternWritable((RuleWritable) b);
            targetPatternB = patternB.makeTargetMarginal();
        }
        else if (b.getClass() == RulePatternWritable.class) {
            // TODO b.makeTargetMarginal() ???
            targetPatternB = (RulePatternWritable) b;
        }
        return targetPatternA.compareTo(targetPatternB);
    }
}
