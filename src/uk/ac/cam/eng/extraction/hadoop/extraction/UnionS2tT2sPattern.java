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
        Map<RulePattern, double[]> union = new HashMap<>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader sequenceReader =
                new SequenceFile.Reader(fs, new Path(s2tInputFile), conf);
        SequenceFile.Reader sequenceReader2 =
                new SequenceFile.Reader(fs, new Path(t2sInputFile), conf);
        // try (BufferedReader brS2t =
        // new BufferedReader(new FileReader(s2tInputFile));
        // BufferedReader brT2s =
        // new BufferedReader(new FileReader(t2sInputFile))) {
        RulePatternWritable key = new RulePatternWritable();
        RulePatternWritable key2 = new RulePatternWritable();
        DoubleArrayWritable value = new DoubleArrayWritable();
        DoubleArrayWritable value2 = new DoubleArrayWritable();
        while (sequenceReader.next(key, value) &&
                sequenceReader2.next(key2, value2)) {
            RulePattern rulePattern = 
            if (union.containsKey(key)) {
                //DoubleWritable[] oldValue = union.get(key).get();
                double[]
                // TODO features config size
                for (int i = 0; i < 2; i++) {
                    oldValue[i].set(oldValue[i].get() + value.get()[i].get());
                }
                union.get(key).set(oldValue);
            }
            else {
                union.put(key, value);
            }
            if (union.containsKey(key2)) {
                DoubleWritable[] oldValue = union.get(key2).get();
                // TODO features config size
                for (int i = 0; i < 2; i++) {
                    oldValue[i].set(oldValue[i].get() + value2.get()[i].get());
                }
                union.get(key2).set(oldValue);
            }
            else {
                union.put(key2, value2);
            }
        }
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
            for (RulePatternWritable rp: union.keySet()) {
                StringBuilder outputLine = new StringBuilder();
                outputLine.append(rp.toString());
                for (DoubleWritable feature: union.get(rp).get()) {
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
