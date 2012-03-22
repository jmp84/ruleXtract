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
public class SourcePatternGroupingComparator extends WritableComparator {

    protected SourcePatternGroupingComparator() {
        super(RuleWritable.class);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.
     * WritableComparable, org.apache.hadoop.io.WritableComparable)
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        RulePatternWritable sourcePatternA = null;
        if (a.getClass() == RuleWritable.class) {
            RulePatternWritable patternA =
                    new RulePatternWritable((RuleWritable) a);
            sourcePatternA = patternA.makeSourceMarginal();
        } else if (a.getClass() == RulePatternWritable.class) {
            sourcePatternA = (RulePatternWritable) a;
        }
        RulePatternWritable sourcePatternB = null;
        if (b.getClass() == RuleWritable.class) {
            RulePatternWritable patternB =
                    new RulePatternWritable((RuleWritable) b);
            sourcePatternB = patternB.makeSourceMarginal();
        } else if (b.getClass() == RulePatternWritable.class) {
            sourcePatternB = (RulePatternWritable) b;
        }
        return sourcePatternA.compareTo(sourcePatternB);
    }
}
