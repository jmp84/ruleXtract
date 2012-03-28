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
public class SourcePatternGroupingComparator extends WritableComparator {

    protected SourcePatternGroupingComparator() {
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
        RuleWritable sourcePatternA = ((RuleWritable) a).getSourcePattern();
        RuleWritable sourcePatternB = ((RuleWritable) b).getSourcePattern();
        return sourcePatternA.compareTo(sourcePatternB);
    }
}
