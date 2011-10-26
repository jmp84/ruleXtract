/**
 * 
 */

package uk.ac.cam.eng.extraction.datatypes;

import java.util.Arrays;

/**
 * @author jmp84 This class represents a sentence
 */
public final class Sentence { // final because immutable class

    private final int[] words;

    /**
     * @return the words
     */
    public int[] getWords() {
        return words;
    }

    public Sentence(String input) {
        String[] parts = input.split("\\s+");
        words = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            words[i] = Integer.parseInt(parts[i]);
        }
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Sentence [words=" + Arrays.toString(words) + "]";
    }

}
