/**
 * 
 */

package uk.ac.cam.eng.extraction.datatypes;

/**
 * @author jmp84 This class represents a sentence pair
 */
public final class SentencePair { // final because immutable class

    /**
     * Source sentence
     */
    private final Sentence source;

    /**
     * Target sentence
     */
    private final Sentence target;

    /**
     * @return the source
     */
    public Sentence getSource() {
        return source;
    }

    /**
     * @return the target
     */
    public Sentence getTarget() {
        return target;
    }

    public SentencePair(String input, boolean side1source) {
        String[] parts = input.split("\n"); // TODO make this format independent
        if (side1source) {
            source = new Sentence(parts[0]);
            target = new Sentence(parts[1]);
        }
        else {
            source = new Sentence(parts[1]);
            target = new Sentence(parts[0]);
        }
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "SentencePair [source=" + source + ", target=" + target + "]";
    }

}
