/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.util.ArrayList;
import java.util.List;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 This class represents a pattern for one side of a rule, e.g.
 *         wXw
 */
public class SidePattern {

    private List<String> pattern;
    private int numberOfNT;

    public SidePattern(List<String> pattern) {
        this.pattern = pattern;
        numberOfNT = 0;
        for (String elt: pattern) {
            if (!elt.equals("w")) {
                numberOfNT++;
            }
        }
    }

    public int size() {
        return pattern.size();
    }

    public String get(int index) {
        return pattern.get(index);
    }

    public static SidePattern parsePattern(String patternString) {
        String[] parts = patternString.split("_");
        List<String> elements = new ArrayList<String>();
        for (String part: parts) {
            if (part.equals("X")) {
                elements.add("-1");
            }
            else if (part.equals("X1")) {
                elements.add("-2");
            }
            else if (part.equals("X2")) {
                elements.add("-3");
            }
            else if (part.equals("W")) {
                elements.add("w");
            }
            else {
                System.err.println("Malformed pattern: " + patternString);
                System.exit(1);
            }
        }
        return new SidePattern(elements);
    }

    public static SidePattern parsePattern2(String patternString) {
        String[] parts = patternString.split("_");
        List<String> elements = new ArrayList<String>();
        for (String part: parts) {
            elements.add(part);
        }
        return new SidePattern(elements);
    }

    private static SidePattern getPattern(String patternString) {
        String parts[] = patternString.split("_");
        List<String> pattern = new ArrayList<String>();
        boolean consecutiveTerminals = false;
        for (String part: parts) {
            if (part.equals("-1") || part.equals("-2") || part.equals("-3")) {
                pattern.add(part);
                consecutiveTerminals = false;
            }
            else {
                if (!consecutiveTerminals) {
                    pattern.add("w");
                }
                consecutiveTerminals = true;
            }
        }
        return new SidePattern(pattern);
    }

    private static SidePattern getPattern(List<Integer> ruleSide) {
        List<String> pattern = new ArrayList<String>();
        boolean consecutiveTerminals = false;
        for (Integer elt: ruleSide) {
            if (elt < 0) {
                pattern.add(elt.toString());
                consecutiveTerminals = false;
            } // TODO change formatting for if else
            else {
                if (!consecutiveTerminals) {
                    pattern.add("w");
                }
                consecutiveTerminals = true;
            }
        }
        return new SidePattern(pattern);
    }

    public static SidePattern getSourcePattern(RuleWritable rule) {
        return getPattern(rule.getSource().toString());
    }

    public static SidePattern getTargetPattern(RuleWritable rule) {
        return getPattern(rule.getTarget().toString());
    }

    public static SidePattern getSourcePattern(Rule rule) {
        return getPattern(rule.getSource());
    }

    public static SidePattern getTargetPattern(Rule rule) {
        return getPattern(rule.getTarget());
    }

    public boolean isPhrase() {
        return (pattern.size() == 1 && pattern.get(0).equals("w"));
    }

    public boolean hasMoreThan1NT() {
        return (numberOfNT > 1);
    }

    public int getFirstNT() {
        for (String elt: pattern) {
            if (!elt.equals("w")) {
                return Integer.parseInt(elt);
            }
        }
        return 0;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((pattern == null) ? 0 : pattern.hashCode());
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
        SidePattern other = (SidePattern) obj;
        if (pattern == null) {
            if (other.pattern != null)
                return false;
        }
        else if (!pattern.equals(other.pattern))
            return false;
        return true;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        if (!pattern.isEmpty()) {
            res.append(pattern.get(0));
        }
        for (int i = 1; i < pattern.size(); i++) {
            res.append("_").append(pattern.get(i));
        }
        return res.toString();
    }
}
