/**
 * 
 */
package uk.ac.cam.eng.rulebuilding.hadoop;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.rulebuilding.retrieval.PatternInstanceCreator2;

/**
 * @author jmp84 This class creates source pattern instances from a test file
 *         and dumps them to a SequenceFile in sorted order for further
 *         processing.
 */
public class SourcePatternInstanceToSequenceFile {

    /**
     * @param args
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        if (args.length != 1) {
            System.err.println("Usage args: configFile");
            System.exit(1);
        }
        String configFile = args[0];
        Properties p = new Properties();
        p.load(new FileInputStream(configFile));
        Configuration conf = new Configuration();
        for (String prop : p.stringPropertyNames()) {
            conf.set(prop, p.getProperty(prop));
        }
        PatternInstanceCreator2 patternInstanceCreator =
                new PatternInstanceCreator2(conf);
        String testFile = conf.get("testfile");
        if (testFile == null) {
            System.err.println("Missing property 'testfile' in the config");
            System.exit(1);
        }
        String outFile = conf.get("source_pattern_instance_sequence_file");
        if (outFile == null) {
            System.err.println("Missing property "
                    + "'source_pattern_instance_sequence_file' in the config");
            System.exit(1);
        }
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outFile);
        Set<Rule> sourcePatternInstances =
                patternInstanceCreator.createSourcePatternInstances(testFile);
        SequenceFile.Writer writer =
                new SequenceFile.Writer(fs, conf, path, RuleWritable.class,
                        NullWritable.class);
        for (Rule sourcePatternInstance : sourcePatternInstances) {
            RuleWritable sourcePatternInstanceWritable =
                    RuleWritable.makeSourceMarginal(sourcePatternInstance);
            writer.append(sourcePatternInstanceWritable, NullWritable.get());
        }
        writer.close();
    }
}
