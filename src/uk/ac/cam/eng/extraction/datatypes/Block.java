/**
 * 
 */

package uk.ac.cam.eng.extraction.datatypes;

/**
 * @author jmp84 This class represents a regular block, that is the widest
 *         phrase pair that can be extracted from a Viterbi alignment in Hiero
 *         extraction
 */
public final class Block { // final because immutable class

    public final int sourceStartIndex;
    public final int sourceEndIndex;
    public final int targetStartIndex;
    public final int targetEndIndex;

    /**
     * @param sourceStartIndex
     * @param sourceEndIndex
     * @param targetStartIndex
     * @param targetEndIndex
     */
    public Block(int sourceStartIndex, int sourceEndIndex,
            int targetStartIndex, int targetEndIndex) {
        this.sourceStartIndex = sourceStartIndex;
        this.sourceEndIndex = sourceEndIndex;
        this.targetStartIndex = targetStartIndex;
        this.targetEndIndex = targetEndIndex;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Block [sourceStartIndex=" + sourceStartIndex
                + ", sourceEndIndex=" + sourceEndIndex + ", targetStartIndex="
                + targetStartIndex + ", targetEndIndex=" + targetEndIndex + "]";
    }

}
