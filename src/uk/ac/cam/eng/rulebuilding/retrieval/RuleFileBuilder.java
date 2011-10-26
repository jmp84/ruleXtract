/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable2;

/**
 * @author jmp84 This class reads a config file specifying an HFile and a test
 *         set and other configurations, retrieves the relevant rules and
 *         returns a rule file ready to be used by the decoder
 */
public class RuleFileBuilder {

    private static byte[] object2ByteArray(Writable obj) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        obj.write(out);
        return buffer.toByteArray();
    }

    private static ArrayWritable convertValueBytes(ByteBuffer bytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes.array(), bytes.arrayOffset(), bytes.limit());
        ArrayWritable value = new ArrayWritable(PairWritable2.class);
        try {
            value.readFields(in);
        }
        catch (IOException e) {
            // Byte buffer is memory backed so no exception is possible. Just in
            // case chain it to a runtime exception
            throw new RuntimeException(e);
        }
        return value;
        /*
         * StringBuilder builder = new StringBuilder(); for (Writable writable :
         * value.get()) { PairWritable2 pair = (PairWritable2) writable;
         * builder.append(pair.toString()); builder.append("\n"); } return
         * builder.toString(); //
         */
    }

    // /*
    private static String printRule(RuleWritable source,
            ArrayWritable listTargetAndProb) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < listTargetAndProb.get().length; i++) {
            PairWritable2 targetAndProb = (PairWritable2) listTargetAndProb
                    .get()[i];
            res.append(source.getLeftHandSide() + " " + source.getSource()
                    + " " + targetAndProb.first.getTarget() + " "
                    + targetAndProb.second + "\n");
        }
        return res.toString();
    }

    // */

    // TODO refactor this main
    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Required arg: config file");
            System.exit(1);
        }
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(args[0]));
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        String testFile = p.getProperty("testfile");
        if (testFile == null) {
            System.err.println("Missing property 'testfile' in the config");
            System.exit(1);
        }
        Integer maxSourcePhrase = Integer.parseInt(p
                .getProperty("max_source_phrase"));
        if (maxSourcePhrase == null) {
            System.err
                    .println("Missing property 'max_source_phrase' in the config");
        }
        String patternFile = p.getProperty("patternfile");
        if (patternFile == null) {
            System.err.println("Missing property 'patternfile' in the config");
            System.exit(1);
        }
        // todo get the rules from test file
        // PatternInstanceCreator patternInstanceCreator = new
        // PatternInstanceCreator();
        // List<SidePattern> sidePatterns =
        // patternInstanceCreator.createSourcePatterns(patternFile);
        PatternInstanceCreator2 patternInstanceCreator = new PatternInstanceCreator2();
        List<SidePattern> sidePatterns = patternInstanceCreator
                .createSourcePatterns(patternFile);
        Set<Rule> sourceRules = patternInstanceCreator
                .createSourcePatternInstances(testFile, sidePatterns);
        System.err.println("source rule size: " + sourceRules.size());
        // System.exit(1);
        // read the HFile and select the rules matching the source phrases and
        // write them to a file
        String outRuleFile = p.getProperty("outrulefile");
        if (outRuleFile == null) {
            System.err.println("Missing property 'outrulefile' in the config");
            System.exit(1);
        }
        // TODO maybe compress directly
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outRuleFile))) {
            String hfile = p.getProperty("hfile");
            if (hfile == null) {
                System.err.println("Missing property 'hfile' in the config");
                System.exit(1);
            }
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            HFile.Reader hfileReader = new HFile.Reader(fs, new Path(hfile),
                    null, false);
            hfileReader.loadFileInfo();
            HFileScanner hfileScanner = hfileReader.getScanner();
            // hfileScanner.seekTo();
            RuleFilter ruleFilter = new RuleFilter();
            ruleFilter.loadConfig(args[0]);
            for (Rule rule: sourceRules) {
                // RuleWritable ruleWritable = new RuleWritable(rule);
                RuleWritable ruleWritable = RuleWritable
                        .makeSourceMarginal(rule);
                byte[] ruleBytes = object2ByteArray(ruleWritable);
                int success = hfileScanner.seekTo(ruleBytes);
                if (success == 0) { // found the source rule
                    // System.err.println("rule found");
                    // ByteBuffer bb = hfileScanner.getValue();
                    // ArrayWritable targetsAndProbs = convertValueBytes(bb);
                    List<PairWritable2> filteredRules = ruleFilter.filter(
                            ruleWritable,
                            convertValueBytes(hfileScanner.getValue()));
                    // List<PairWritable2> filteredRules =
                    // ruleFilter.filter(ruleWritable, targetsAndProbs);
                    // if (targetsAndProbs.get().length > 1) {
                    // System.err.println("source: " + ruleWritable.toString());
                    // System.err.println("targets and probs:");
                    // for (int i = 0; i < targetsAndProbs.get().length; i++) {
                    // System.err.print(" " +
                    // ((PairWritable2)targetsAndProbs.get()[i]).toString());
                    // }
                    // System.err.println();
                    // System.err.println("filtered: " + filteredRules);
                    // }
                    // bw.write(printRule(ruleWritable,
                    // convertValueBytes(hfileScanner.getValue())));
                    // bw.write(printRule(ruleWritable, targetsAndProbs));
                    for (PairWritable2 ruleAndProb: filteredRules) {
                        bw.write(ruleAndProb.toString() + "\n");
                    }
                }
                // else {
                // System.err.println("rule not found");
                // }
            }
        }
    }
}
