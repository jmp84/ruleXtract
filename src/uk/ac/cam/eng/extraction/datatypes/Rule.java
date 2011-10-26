/**
 * 
 */

package uk.ac.cam.eng.extraction.datatypes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jmp84 This class describes a phrasal or hierarchical rule
 */
public final class Rule { // final because immutable class

    // TODO when extending, find another way
    // TODO introduce boolean saying monotonic or not ?
    private final static int X = -1;
    private final static int X1 = -2;
    private final static int X2 = -3;
    private final static int S = -4;
    private final static int V = -5;

    /**
     * Number of nonterminals in the rule, can be 0, 1 or 2 for now
     */
    private int nbNonTerminal;

    /**
     * Left hand side of the rule (X typically)
     */
    private final int leftHandSide;

    /**
     * Source side of the rule
     */
    private final List<Integer> source;

    /**
     * Target side of the rule
     */
    private final List<Integer> target;

    public Rule(List<Integer> src, List<Integer> trg) {
        this.leftHandSide = 0; // TODO modify this to make general
        this.source = new ArrayList<Integer>(src);
        this.target = new ArrayList<Integer>(trg);
    }

    /**
     * @param sourceStartIndex
     * @param sourceEndIndex
     * @param targetStartIndex
     * @param targetExtendIndex
     * @param sp
     */
    public Rule(int sourceStartIndex, int sourceEndIndex, int targetStartIndex,
            int targetEndIndex, SentencePair sp) {
        this.leftHandSide = 0;
        this.nbNonTerminal = 0;
        source = new ArrayList<Integer>();
        for (int sourceIndex = sourceStartIndex; sourceIndex <= sourceEndIndex; sourceIndex++) {
            source.add(sp.getSource().getWords()[sourceIndex]);
        }
        target = new ArrayList<Integer>();
        for (int targetIndex = targetStartIndex; targetIndex <= targetEndIndex; targetIndex++) {
            target.add(sp.getTarget().getWords()[targetIndex]);
        }
    }

    /**
     * @param sourceStartIndex
     * @param sourceEndIndex
     * @param minTargetIndex
     * @param maxTargetIndex
     * @param sourceStartIndexX
     * @param sourceEndIndexX
     * @param minTargetIndexX
     * @param maxTargetIndexX
     * @param sp
     */
    public Rule(int sourceStartIndex, int sourceEndIndex, int minTargetIndex,
            int maxTargetIndex, int sourceStartIndexX, int sourceEndIndexX,
            int minTargetIndexX, int maxTargetIndexX, SentencePair sp) {
        this.leftHandSide = 0;
        this.nbNonTerminal = 1;
        source = new ArrayList<Integer>();
        for (int sourceIndex = sourceStartIndex; sourceIndex < sourceStartIndexX; sourceIndex++) {
            source.add(sp.getSource().getWords()[sourceIndex]);
        }
        source.add(X);
        for (int sourceIndex = sourceEndIndexX + 1; sourceIndex <= sourceEndIndex; sourceIndex++) {
            source.add(sp.getSource().getWords()[sourceIndex]);
        }
        target = new ArrayList<Integer>();
        for (int targetIndex = minTargetIndex; targetIndex < minTargetIndexX; targetIndex++) {
            target.add(sp.getTarget().getWords()[targetIndex]);
        }
        target.add(X);
        for (int targetIndex = maxTargetIndexX + 1; targetIndex <= maxTargetIndex; targetIndex++) {
            target.add(sp.getTarget().getWords()[targetIndex]);
        }
    }

    /**
     * @param sourceStartIndex
     * @param sourceEndIndex
     * @param minTargetIndex
     * @param maxTargetIndex
     * @param sourceStartIndexX
     * @param sourceEndIndexX
     * @param minTargetIndexX
     * @param maxTargetIndexX
     * @param sourceStartIndexX2
     * @param sourceEndIndexX2
     * @param minTargetIndexX2
     * @param maxTargetIndexX2
     * @param sp
     */
    public Rule(int sourceStartIndex, int sourceEndIndex, int minTargetIndex,
            int maxTargetIndex, int sourceStartIndexX, int sourceEndIndexX,
            int minTargetIndexX, int maxTargetIndexX, int sourceStartIndexX2,
            int sourceEndIndexX2, int minTargetIndexX2, int maxTargetIndexX2,
            SentencePair sp) {
        this.leftHandSide = 0;
        this.nbNonTerminal = 2;
        source = new ArrayList<Integer>();
        target = new ArrayList<Integer>();
        if (minTargetIndexX2 > maxTargetIndexX) {
            for (int sourceIndex = sourceStartIndex; sourceIndex < sourceStartIndexX; sourceIndex++) {
                source.add(sp.getSource().getWords()[sourceIndex]);
            }
            source.add(X1);
            for (int sourceIndex = sourceEndIndexX + 1; sourceIndex < sourceStartIndexX2; sourceIndex++) {
                source.add(sp.getSource().getWords()[sourceIndex]);
            }
            source.add(X2);
            for (int sourceIndex = sourceEndIndexX2 + 1; sourceIndex <= sourceEndIndex; sourceIndex++) {
                source.add(sp.getSource().getWords()[sourceIndex]);
            }
            for (int targetIndex = minTargetIndex; targetIndex < minTargetIndexX; targetIndex++) {
                target.add(sp.getTarget().getWords()[targetIndex]);
            }
            target.add(X1);
            for (int targetIndex = maxTargetIndexX + 1; targetIndex < minTargetIndexX2; targetIndex++) {
                target.add(sp.getTarget().getWords()[targetIndex]);
            }
            target.add(X2);
            for (int targetIndex = maxTargetIndexX2 + 1; targetIndex <= maxTargetIndex; targetIndex++) {
                target.add(sp.getTarget().getWords()[targetIndex]);
            }
        }
        // when there is a non terminal swap, we prefer having the nonterminals
        // ordered in the source
        // (X1 ... X2) and swaped in the target (X2 ... X1) because otherwise,
        // when computing
        // source-to-target probability, the denominator would include two kinds
        // of source, the sources
        // with non terminal in order and the sources with nonterminal swaped.
        // TODO when we compute target-to-source probabilities, we have the same
        // problem, add an option
        // to decide how to print the rule depending we're doing a hadoop source
        // to target job or a
        // hadoop target to source job.
        else {
            for (int sourceIndex = sourceStartIndex; sourceIndex < sourceStartIndexX; sourceIndex++) {
                source.add(sp.getSource().getWords()[sourceIndex]);
            }
            source.add(X1);
            for (int sourceIndex = sourceEndIndexX + 1; sourceIndex < sourceStartIndexX2; sourceIndex++) {
                source.add(sp.getSource().getWords()[sourceIndex]);
            }
            source.add(X2);
            for (int sourceIndex = sourceEndIndexX2 + 1; sourceIndex <= sourceEndIndex; sourceIndex++) {
                source.add(sp.getSource().getWords()[sourceIndex]);
            }
            for (int targetIndex = minTargetIndex; targetIndex < minTargetIndexX2; targetIndex++) {
                target.add(sp.getTarget().getWords()[targetIndex]);
            }
            target.add(X2);
            for (int targetIndex = maxTargetIndexX2 + 1; targetIndex < minTargetIndexX; targetIndex++) {
                target.add(sp.getTarget().getWords()[targetIndex]);
            }
            target.add(X1);
            for (int targetIndex = maxTargetIndexX + 1; targetIndex <= maxTargetIndex; targetIndex++) {
                target.add(sp.getTarget().getWords()[targetIndex]);
            }
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(leftHandSide);
        sb.append(" ");
        for (int i = 0; i < source.size(); i++) {
            sb.append(source.get(i) + "_");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" ");
        for (int i = 0; i < target.size(); i++) {
            sb.append(target.get(i) + "_");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    /**
     * This method counts the number of target words. It is used for the word
     * insertion penalty feature.
     * 
     * @return The number of target words.
     */
    public int nbTargetWords() {
        return target.size() - nbNonTerminal;
    }

    /**
     * Decides if the rule is a concatenating glue rule (S-->SX,SX) or not
     * 
     * @return True if the rule is S-->SX,SX
     */
    public boolean isConcatenatingGlue() {
        if (source.size() == 2 && source.get(0) == S && source.get(1) == X
                && target.size() == 2 && target.get(0) == S
                && target.get(1) == X)
            return true;
        return false;
    }

    /**
     * @return
     */
    public boolean isStartingGlue() {
        if (source.size() == 1 && source.get(0) == V && target.size() == 1
                && target.get(0) == V)
            return true;
        return false;
    }

    public List<Integer> getSource() {
        return source;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + leftHandSide;
        result = prime * result + ((source == null) ? 0 : source.hashCode());
        result = prime * result + ((target == null) ? 0 : target.hashCode());
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
        Rule other = (Rule) obj;
        if (leftHandSide != other.leftHandSide)
            return false;
        if (source == null) {
            if (other.source != null)
                return false;
        }
        else if (!source.equals(other.source))
            return false;
        if (target == null) {
            if (other.target != null)
                return false;
        }
        else if (!target.equals(other.target))
            return false;
        return true;
    }
}
