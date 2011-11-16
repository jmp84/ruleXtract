/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.rulebuilding.features.FeatureCreator;

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

    private Set<Rule> getAsciiConstraints(String filename) throws IOException {
        Set<Rule> res = new HashSet<Rule>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            Pattern regex = Pattern.compile(".*: (.*) # (.*)");
            Matcher matcher;
            while ((line = br.readLine()) != null) {
                matcher = regex.matcher(line);
                if (matcher.matches()) {
                    String[] sourceString = matcher.group(1).split(" ");
                    String[] targetString = matcher.group(2).split(" ");
                    List<Integer> source = new ArrayList<Integer>();
                    List<Integer> target = new ArrayList<Integer>();
                    for (String ss: sourceString) {
                        source.add(Integer.parseInt(ss));
                    }
                    for (String ts: targetString) {
                        target.add(Integer.parseInt(ts));
                    }
                    Rule rule = new Rule(source, target);
                    res.add(rule);
                }
                else {
                    System.err.println("Malformed ascii constraint file: "
                            + filename);
                    System.exit(1);
                }
            }
        }
        return res;
    }

    private Set<Integer> getTestVocab(String testFile)
            throws FileNotFoundException, IOException {
        Set<Integer> res = new HashSet<Integer>();
        try (BufferedReader br = new BufferedReader(new FileReader(testFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+");
                for (String part: parts) {
                    res.add(Integer.parseInt(part));
                }
            }
        }
        return res;
    }

    /**
     * @param testFile
     * @param hfile
     * @return
     * @throws IOException
     */
    private List<PairWritable3> getAsciiOovDeletionRules(String testFile,
            String hfile, String asciiConstraints) throws IOException {
        Set<Rule> asciiRules = getAsciiConstraints(asciiConstraints);
        List<PairWritable3> res = new ArrayList<PairWritable3>();
        // read the HFile and select the rules matching the source phrases
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        HFile.Reader hfileReader = new HFile.Reader(fs, new Path(hfile),
                null, false);
        hfileReader.loadFileInfo();
        HFileScanner hfileScanner = hfileReader.getScanner();
        Set<Integer> testVocab = getTestVocab(testFile);
        for (Rule asciiRule: asciiRules) {
            res.add(new PairWritable3(new RuleWritable(asciiRule),
                    new ArrayWritable(DoubleWritable.class)));
        }
        for (Integer testWord: testVocab) {
            // for (Rule rule: sourceRules) {
            List<Integer> source = new ArrayList<Integer>();
            source.add(testWord);
            Rule rule = new Rule(source, new ArrayList<Integer>());
            Rule asciiRule = new Rule(source, source);
            if (asciiRules.contains(asciiRule)) {
                continue;
            }
            RuleWritable ruleWritable = RuleWritable
                    .makeSourceMarginal(rule);
            byte[] ruleBytes = object2ByteArray(ruleWritable);
            int success = hfileScanner.seekTo(ruleBytes);
            if (success != 0) { // did not found the source: add an oov rule
                res.add(new PairWritable3(new RuleWritable(rule),
                        new ArrayWritable(DoubleWritable.class)));
            }
            else { // found it: add deletion rule
                List<Integer> deletion = new ArrayList<Integer>();
                // deletion is represented by a zero
                deletion.add(0);
                Rule deletionRule = new Rule(source, deletion);
                RuleWritable deletionRuleWritable =
                        new RuleWritable(deletionRule);
                res.add(new PairWritable3(deletionRuleWritable,
                        new ArrayWritable(DoubleWritable.class)));
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
        String source2TargetLexicalModel =
                p.getProperty("source2target_lexical_model");
        if (source2TargetLexicalModel == null) {
            System.err.println("Missing property " +
                    "'source2target_lexical_model' in the config");
            System.exit(1);
        }
        String target2SourceLexicalModel =
                p.getProperty("target2source_lexical_model");
        if (target2SourceLexicalModel == null) {
            System.err.println("Missing property " +
                    "'target2source_lexical_model' in the config");
            System.exit(1);
        }
        String selectedFeaturesString = p.getProperty("features");
        if (selectedFeaturesString == null) {
            System.err.println("Missing property 'features' in the config");
            System.exit(1);
        }
        String[] selectedFeatures = selectedFeaturesString.split(",");
        FeatureCreator featureCreator =
                new FeatureCreator(source2TargetLexicalModel,
                        target2SourceLexicalModel, rules, selectedFeatures);
        List<PairWritable3> rulesWithFeatures =
                featureCreator.createFeatures(rules);
        String asciiConstraints = p.getProperty("ascii_constraints");
        if (asciiConstraints == null) {
            System.err
                    .println("Missing property 'ascii_constraints' in the config");
            System.exit(1);
        }
        List<PairWritable3> asciiOovDeletionRules =
                ruleFileBuilder.getAsciiOovDeletionRules(testFile, hfile,
                        asciiConstraints);
        List<PairWritable3> asciiOovDeletionRulesWithFeatures =
                featureCreator
                        .createFeaturesAsciiOovDeletion(asciiOovDeletionRules);
        try (BufferedOutputStream bos =
                new BufferedOutputStream(new GZIPOutputStream(
                        new FileOutputStream(outRuleFile)))) {
            for (PairWritable3 ruleWithFeatures: rulesWithFeatures) {
                // bw.write((ruleWithFeatures.toString() + "\n").getBytes());
                bos.write(ruleWithFeatures.first.toString().getBytes());
                Writable[] features = ruleWithFeatures.second.get();
                for (Writable w: features) {
                    bos.write((" " + w.toString()).getBytes());
                }
                bos.write("\n".getBytes());
            }
            for (PairWritable3 asciiOovDeletionRuleWithFeatures: asciiOovDeletionRulesWithFeatures) {
                bos.write(asciiOovDeletionRuleWithFeatures.first.toString()
                        .getBytes());
                Writable[] features =
                        asciiOovDeletionRuleWithFeatures.second.get();
                for (Writable w: features) {
                    bos.write((" " + w.toString()).getBytes());
                }
                bos.write("\n".getBytes());
            }
        }
    }
}
