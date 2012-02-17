/**
 * 
 */

package uk.ac.cam.eng.util;

/**
 * @author jmp84
 */
public class Pair<T, U> {

    private T first;
    private U second;

    public Pair() {}

    /**
     * @param first
     * @param second
     */
    public Pair(T first, U second) {
        this.first = first;
        this.second = second;
    }

    /**
     * @return the first
     */
    public T getFirst() {
        return first;
    }

    /**
     * @param first
     *            the first to set
     */
    public void setFirst(T first) {
        this.first = first;
    }

    /**
     * @return the second
     */
    public U getSecond() {
        return second;
    }

    /**
     * @param second
     *            the second to set
     */
    public void setSecond(U second) {
        this.second = second;
    }
}
