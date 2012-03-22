/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import org.apache.hadoop.io.Text;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.rulebuilding.retrieval.RulePattern;

/**
 * @author jmp84 This class represents a writable rule pattern. It is used for
 *         extracting rule pattern probability with MapReduce.
 */
public class RulePatternWritable extends RuleWritable {

    public RulePatternWritable(RuleWritable ruleWritable) {
        Rule rule = new Rule(ruleWritable);
        RulePattern rulePattern = RulePattern.getPattern(rule);
        String[] parts = rulePattern.toString().split("\\s+");
        if (parts.length != 2) {
            System.err.println("Rule pattern malformed: "
                    + rulePattern.toString());
            System.exit(1);
        }
        leftHandSide = new Text();
        source = new Text(parts[0]);
        target = new Text(parts[1]);
    }

    public RulePatternWritable makeSourceMarginal() {
        target.clear();
        return this;
    }

    public RulePatternWritable makeTargetMarginal() {
        source.clear();
        return this;
    }

    @Override
    public boolean isPattern() {
        return true;
    }

    public boolean isSourceEmpty() {
        return source.toString().isEmpty();
    }

    public boolean isTargetEmpty() {
        return target.toString().isEmpty();
    }
}
