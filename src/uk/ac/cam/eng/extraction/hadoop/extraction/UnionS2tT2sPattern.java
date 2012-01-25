/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import uk.ac.cam.eng.rulebuilding.retrieval.RulePattern;

/**
 * @author jmp84 This class is a utility to take the output of the
 *         source-to-target and target-to-source pattern extraction and union
 *         them.
 */
public class UnionS2tT2sPattern {

    public static void union(
            String s2tInputFile, String t2sInputFile, String outputFile)
            throws FileNotFoundException, IOException {
        Map<RulePattern, double[]> union = new HashMap<RulePattern, double[]>();
        try (BufferedReader brS2t =
                new BufferedReader(new FileReader(s2tInputFile));
                BufferedReader brT2s =
                        new BufferedReader(new FileReader(t2sInputFile))) {
            String lineS2t = null, lineT2s = null;
            while ((lineS2t = brS2t.readLine()) != null
                    && (lineT2s = brT2s.readLine()) != null) {
                String[] partsS2t = lineS2t.split("\\s+");
                String[] partsT2s = lineT2s.split("\\s+");
                if (partsS2t.length != partsT2s.length) {
                    System.err.println(
                            "Malformed lines: \n" + lineS2t + "\n" + lineT2s);
                    System.exit(1);
                }
                RulePattern s2tPattern =
                        RulePattern.parsePattern(partsS2t[0], partsS2t[1]);
                if (union.containsKey(s2tPattern)) {
                    double[] value = union.get(s2tPattern);
                    // TODO features config size
                    for (int i = 0; i < 2; i++) {
                        value[i] += Double.parseDouble(partsS2t[i + 2]);
                    }
                    union.put(s2tPattern, value);
                }
                else {
                    double[] value = new double[partsS2t.length - 2];
                    for (int i = 0; i < value.length; i++) {
                        value[i] = Double.parseDouble(partsS2t[i + 2]);
                    }
                    union.put(s2tPattern, value);
                }
                RulePattern t2sPattern =
                        RulePattern.parsePattern(partsT2s[0], partsT2s[1]);
                if (union.containsKey(t2sPattern)) {
                    double[] value = union.get(t2sPattern);
                    // TODO features config size
                    for (int i = 0; i < 2; i++) {
                        value[i] += Double.parseDouble(partsT2s[i + 2]);
                    }
                    union.put(t2sPattern, value);
                }
                else {
                    double[] value = new double[partsT2s.length - 2];
                    for (int i = 0; i < value.length; i++) {
                        value[i] = Double.parseDouble(partsT2s[i + 2]);
                    }
                    union.put(t2sPattern, value);
                }
            }
        }
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
            for (RulePattern rp: union.keySet()) {
                StringBuilder outputLine = new StringBuilder();
                outputLine.append(rp.toString());
                for (double feature: union.get(rp)) {
                    outputLine.append(" " + feature);
                }
                bw.write(outputLine.toString() + "\n");
            }
        }
    }

    /**
     * @param args
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        if (args.length != 3) {
            System.err.println("Usage: UnionS2tT2sPattern source2targetInput " +
                    "target2sourceInput outputFile");
            System.exit(1);
        }
        union(args[0], args[1], args[2]);
    }

}
