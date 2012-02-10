/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;

import uk.ac.cam.eng.extraction.hadoop.datatypes.DoubleArrayWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RulePatternWritable;
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
        Map<RulePattern, DoubleWritable[]> union = new HashMap<>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader sequenceReader =
                new SequenceFile.Reader(fs, new Path(s2tInputFile), conf);
        SequenceFile.Reader sequenceReader2 =
                new SequenceFile.Reader(fs, new Path(t2sInputFile), conf);
        RulePatternWritable key = new RulePatternWritable();
        RulePatternWritable key2 = new RulePatternWritable();
        DoubleArrayWritable value = new DoubleArrayWritable();
        DoubleArrayWritable value2 = new DoubleArrayWritable();
        while (sequenceReader.next(key, value) &&
                sequenceReader2.next(key2, value2)) {
            RulePattern rulePattern = RulePattern.getPattern(key);
            RulePattern rulePattern2 = RulePattern.getPattern(key2);
            if (union.containsKey(rulePattern)) {
                DoubleWritable[] oldValue = union.get(rulePattern);
                for (int i = 0; i < 2; i++) {
                    oldValue[i].set(oldValue[i].get() + value.get()[i].get());
                }
            }
            else {
                union.put(rulePattern, value.get());
            }
            // TODO duplicated code
            if (union.containsKey(rulePattern2)) {
                DoubleWritable[] oldValue = union.get(rulePattern2);
                for (int i = 0; i < 2; i++) {
                    oldValue[i].set(oldValue[i].get() + value2.get()[i].get());
                }
                // union.get(key2).set(oldValue);
            }
            else {
                union.put(rulePattern2, value2.get());
            }
        }
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
            for (RulePattern rp: union.keySet()) {
                StringBuilder outputLine = new StringBuilder();
                outputLine.append(rp.toString());
                for (DoubleWritable feature: union.get(rp)) {
                    outputLine.append(" " + feature.get());
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
