/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 This class reads a config file specifying an HFile and a test
 *         set and other configurations, retrieves the relevant rules and
 *         returns a rule file ready to be used by the decoder
 */
public class RuleFileBuilder {

    private RuleFilter ruleFilter;
    private PatternInstanceCreator2 patternInstanceCreator;

    public RuleFileBuilder(String filterConfig) throws FileNotFoundException,
            IOException {
        ruleFilter = new RuleFilter();
        ruleFilter.loadConfig(filterConfig);
        patternInstanceCreator = new PatternInstanceCreator2();
    }

    private static byte[] object2ByteArray(Writable obj) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        obj.write(out);
        return buffer.toByteArray();
    }

    private static ArrayWritable convertValueBytes(ByteBuffer bytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes.array(), bytes.arrayOffset(), bytes.limit());
        ArrayWritable value = new ArrayWritable(PairWritable3.class);
        try {
            value.readFields(in);
        }
        catch (IOException e) {
            // Byte buffer is memory backed so no exception is possible. Just in
            // case chain it to a runtime exception
            throw new RuntimeException(e);
        }
        return value;
    }

    Set<Rule> getSourceRuleInstances(String patternFile, String testFile)
            throws FileNotFoundException, IOException {
        List<SidePattern> sidePatterns = patternInstanceCreator
                .createSourcePatterns(patternFile);
        return patternInstanceCreator.createSourcePatternInstances(testFile,
                sidePatterns);
    }

    private List<PairWritable3> getRules(String patternFile,
            String testFile, String hfile) throws IOException {
        List<PairWritable3> res = new ArrayList<PairWritable3>();
        Set<Rule> sourceRules = getSourceRuleInstances(patternFile, testFile);
        System.err.println("source rule size: " + sourceRules.size());
        // read the HFile and select the rules matching the source phrases
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        HFile.Reader hfileReader = new HFile.Reader(fs, new Path(hfile),
                null, false);
        hfileReader.loadFileInfo();
        HFileScanner hfileScanner = hfileReader.getScanner();
        for (Rule rule: sourceRules) {
            RuleWritable ruleWritable = RuleWritable
                    .makeSourceMarginal(rule);
            byte[] ruleBytes = object2ByteArray(ruleWritable);
            int success = hfileScanner.seekTo(ruleBytes);
            if (success == 0) { // found the source rule
                List<PairWritable3> filteredRules = ruleFilter.filter(
                        ruleWritable,
                        convertValueBytes(hfileScanner.getValue()));
                res.addAll(filteredRules);
            }
        }
        return res;
    }

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
        RuleFileBuilder ruleFileBuilder = new RuleFileBuilder(args[0]);
        // read the HFile and select the rules matching the source phrases and
        // write them to a file
        String outRuleFile = p.getProperty("outrulefile");
        if (outRuleFile == null) {
            System.err.println("Missing property 'outrulefile' in the config");
            System.exit(1);
        }
        String hfile = p.getProperty("hfile");
        if (hfile == null) {
            System.err.println("Missing property 'hfile' in the config");
            System.exit(1);
        }
        List<PairWritable3> rules =
                ruleFileBuilder.getRules(patternFile, testFile, hfile);
        try (GZIPOutputStream bw =
                new GZIPOutputStream(new FileOutputStream(outRuleFile))) {
            for (PairWritable3 ruleAndProb: rules) {
                bw.write((ruleAndProb.toString() + "\n").getBytes());
            }
        }
    }
}
