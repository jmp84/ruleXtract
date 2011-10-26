/**
 * 
 */

package uk.ac.cam.eng.extraction.datatypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author jmp84
 */
public final class Alignment {

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Alignment [s2t=" + s2t + "]";
    }

    private static Logger logger = Logger
            .getLogger("uk.ac.cam.eng.extraction.alignment");

    /**
     * set of links in a sentence pair Viterbi aligned arranged by source index
     */
    // private TreeMap<Integer, TreeSet<Integer>> s2t;
    // private Map<Integer, List<Integer>> s2t;
    // private Map<Integer, Integer> s2tmax;
    // private Map<Integer, Integer> s2tmin;
    private final List<List<Integer>> s2t;
    private final List<Integer> s2tmax;
    private final List<Integer> s2tmin;

    /**
     * set of links in a sentence pair Viterbi aligned arranged by target index
     */
    // private TreeMap<Integer, TreeSet<Integer>> t2s;
    // private Map<Integer, List<Integer>> t2s;
    // private Map<Integer, Integer> t2smax;
    // private Map<Integer, Integer> t2smin;
    private final List<List<Integer>> t2s;
    private final List<Integer> t2smax;
    private final List<Integer> t2smin;

    /**
     * This constructor reads an alignment in acn format and fills the fields
     * f2e and e2f
     * 
     * @param acnAlign
     *            alignment in acn format
     * @param side1source
     *            1 or 2
     */
    public Alignment(String acnAlign, SentencePair sp, boolean side1source) {
        // s2t = new TreeMap<Integer, TreeSet<Integer>>();
        // t2s = new TreeMap<Integer, TreeSet<Integer>>();
        // s2t = new HashMap<Integer, List<Integer>>();
        // t2s = new HashMap<Integer, List<Integer>>();
        // s2tmax = new HashMap<Integer, Integer>();
        // s2tmin = new HashMap<Integer, Integer>();
        // t2smax = new HashMap<Integer, Integer>();
        // t2smin = new HashMap<Integer, Integer>();
        s2t = new ArrayList<List<Integer>>(sp.getSource().getWords().length);
        for (int i = 0; i < sp.getSource().getWords().length; i++) {
            s2t.add(null);
        }
        s2tmax = new ArrayList<Integer>(sp.getSource().getWords().length);
        for (int i = 0; i < sp.getSource().getWords().length; i++) {
            s2tmax.add(Integer.MIN_VALUE);
        }
        s2tmin = new ArrayList<Integer>(sp.getSource().getWords().length);
        for (int i = 0; i < sp.getSource().getWords().length; i++) {
            s2tmin.add(Integer.MAX_VALUE);
        }
        t2s = new ArrayList<List<Integer>>(sp.getTarget().getWords().length);
        for (int i = 0; i < sp.getTarget().getWords().length; i++) {
            t2s.add(null);
        }
        t2smax = new ArrayList<Integer>(sp.getTarget().getWords().length);
        for (int i = 0; i < sp.getTarget().getWords().length; i++) {
            t2smax.add(Integer.MIN_VALUE);
        }
        t2smin = new ArrayList<Integer>(sp.getTarget().getWords().length);
        for (int i = 0; i < sp.getTarget().getWords().length; i++) {
            t2smin.add(Integer.MAX_VALUE);
        }
        // handle empty alignment case
        String[] lines = new String[0]; // empty array, not null though
        if (!acnAlign.equals("")) {
            lines = acnAlign.split("\\n");
        }
        for (String line: lines) {
            String[] parts = line.split("\\s+");
            if (parts.length != 3) {
                logger.log(Level.SEVERE, "Alignment bad format: " + line + "\n"
                        + "acnalign: " + acnAlign + "\n" + "sentencepair: "
                        + sp);
                System.exit(1);
            }
            int sourcePosition = -1, targetPosition = -1;
            if (side1source) {
                sourcePosition = Integer.parseInt(parts[1]);
                targetPosition = Integer.parseInt(parts[2]);
            }
            else {
                sourcePosition = Integer.parseInt(parts[2]);
                targetPosition = Integer.parseInt(parts[1]);
            }
            // sourcePosition++; // take into account null word
            // targetPosition++; // take into account null word
            // if (s2t.containsKey(sourcePosition)) {
            if (s2t.get(sourcePosition) != null) {
                s2t.get(sourcePosition).add(targetPosition);
            }
            else {
                // TreeSet<Integer> newEntry = new TreeSet<Integer>();
                List<Integer> newEntry = new ArrayList<Integer>();
                newEntry.add(targetPosition);
                // s2t.put(sourcePosition, newEntry);
                s2t.set(sourcePosition, newEntry);
            }
            // if (s2tmax.containsKey(sourcePosition)) {
            // if (s2tmax.get(sourcePosition) != null) {
            if (targetPosition > s2tmax.get(sourcePosition)) {
                // s2tmax.put(sourcePosition, targetPosition);
                s2tmax.set(sourcePosition, targetPosition);
            }
            // }
            // else {
            // s2tmax.put(sourcePosition, targetPosition);
            // s2tmax.set(sourcePosition, targetPosition);
            // }
            // if (s2tmin.containsKey(sourcePosition)) {
            // if (s2tmin.get(sourcePosition) != null) {
            if (targetPosition < s2tmin.get(sourcePosition)) {
                // s2tmin.put(sourcePosition, targetPosition);
                s2tmin.set(sourcePosition, targetPosition);
            }
            // }
            // else {
            // s2tmin.put(sourcePosition, targetPosition);
            // s2tmin.set(sourcePosition, targetPosition);
            // }
            // if (t2s.containsKey(targetPosition)) {
            if (t2s.get(targetPosition) != null) {
                t2s.get(targetPosition).add(sourcePosition);
            }
            else {
                // TreeSet<Integer> newEntry = new TreeSet<Integer>();
                List<Integer> newEntry = new ArrayList<Integer>();
                newEntry.add(sourcePosition);
                // t2s.put(targetPosition, newEntry);
                t2s.set(targetPosition, newEntry);
            }
            // if (t2smax.containsKey(targetPosition)) {
            // if (t2smax.get(targetPosition) != null) {
            if (sourcePosition > t2smax.get(targetPosition)) {
                // t2smax.put(targetPosition, sourcePosition);
                t2smax.set(targetPosition, sourcePosition);
            }
            // }
            // else {
            // t2smax.put(targetPosition, sourcePosition);
            // t2smax.set(targetPosition, sourcePosition);
            // }
            // if (t2smin.containsKey(targetPosition)) {
            // if (t2smin.get(targetPosition) != null) {
            if (sourcePosition < t2smin.get(targetPosition)) {
                // t2smin.put(targetPosition, sourcePosition);
                t2smin.set(targetPosition, sourcePosition);
            }
            // }
            // else {
            // t2smin.put(targetPosition, sourcePosition);
            // t2smin.set(targetPosition, sourcePosition);
            // }
        }
    }

    public int getMinTargetIndex(int sourceIndex) {
        // if (!s2t.containsKey(sourceIndex)) // source index unaligned
        // if (s2t.get(sourceIndex) == null)
        // return Integer.MAX_VALUE;
        // return s2t.get(sourceIndex).first();
        return s2tmin.get(sourceIndex);
    }

    public int getMaxTargetIndex(int sourceIndex) {
        // if (!s2t.containsKey(sourceIndex)) // source index unaligned
        // if (s2t.get(sourceIndex) == null)
        // return Integer.MIN_VALUE;
        // return s2t.get(sourceIndex).last();
        return s2tmax.get(sourceIndex);
    }

    public int getMinSourceIndex(int targetIndex) {
        // if (!t2s.containsKey(targetIndex)) // source index unaligned
        // if (t2s.get(targetIndex) == null)
        // return Integer.MAX_VALUE;
        // return t2s.get(targetIndex).first();
        return t2smin.get(targetIndex);
    }

    public int getMaxSourceIndex(int targetIndex) {
        // if (!t2s.containsKey(targetIndex)) // source index unaligned
        // if (t2s.get(targetIndex) == null)
        // return Integer.MIN_VALUE;
        // return t2s.get(targetIndex).last();
        return t2smax.get(targetIndex);
    }

    public boolean isSourceAligned(int sourceIndex) {
        // return s2t.containsKey(sourceIndex);
        return (s2t.get(sourceIndex) != null);
    }

    public boolean isTargetAligned(int targetIndex) {
        // return t2s.containsKey(targetIndex);
        return (t2s.get(targetIndex) != null);
    }

}
