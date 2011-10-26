/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.util.ArrayList;
import java.util.List;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 This class represents a pattern (e.g. wXw-wXw)
 */
public class RulePattern {

    private SidePattern sourcePattern;
    private SidePattern targetPattern;

    public RulePattern(SidePattern sourcePattern, SidePattern targetPattern) {
        this.sourcePattern = sourcePattern;
        this.targetPattern = targetPattern;
    }

    public static RulePattern parsePattern(String patternString) {
        // X_W-W_X
        String[] sourceTarget = patternString.split("-");
        if (sourceTarget.length != 2) {
            System.err.println("Malformed pattern: " + patternString);
            System.exit(1);
        }
        return new RulePattern(SidePattern.parsePattern(sourceTarget[0]),
                SidePattern.parsePattern(sourceTarget[1]));
    }

    public static RulePattern getPattern(RuleWritable source,
            RuleWritable target) {
        return new RulePattern(SidePattern.getSourcePattern(source),
                SidePattern.getTargetPattern(target));
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((sourcePattern == null) ? 0 : sourcePattern.hashCode());
        result = prime * result
                + ((targetPattern == null) ? 0 : targetPattern.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RulePattern other = (RulePattern) obj;
        if (sourcePattern == null) {
            if (other.sourcePattern != null)
                return false;
        }
        else if (!sourcePattern.equals(other.sourcePattern))
            return false;
        if (targetPattern == null) {
            if (other.targetPattern != null)
                return false;
        }
        else if (!targetPattern.equals(other.targetPattern))
            return false;
        return true;
    }
}
