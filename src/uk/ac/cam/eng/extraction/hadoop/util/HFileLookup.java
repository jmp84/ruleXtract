/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Utility to look up a source in an hfile
 */
public class HFileLookup {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        HFile.Reader hfileReader =
                HFile.createReader(fs, new Path(args[0]), new CacheConfig(conf));
        hfileReader.loadFileInfo();
        HFileScanner hfileScanner = hfileReader.getScanner(false, false, false);
        Rule rule = new Rule(args[1], "");
        RuleWritable ruleWritable = RuleWritable.makeSourceMarginal(rule);
        byte[] ruleBytes = Util.object2ByteArray(ruleWritable);
        int success = hfileScanner.seekTo(ruleBytes);
        if (success == 0) { // found the source rule
            ArrayWritable targetsAndFeatures =
                    Util.bytes2ArrayWritable(hfileScanner.getValue());
            for (int i = 0; i < targetsAndFeatures.get().length; i++) {
                GeneralPairWritable3 elt =
                        (GeneralPairWritable3) targetsAndFeatures.get()[i];
                String out =
                        ruleWritable.toString() + " "
                                + elt.getFirst().toString();
                SortedMapWritable features = elt.getSecond();
                for (Writable featureIndex: features.keySet()) {
                    out += " " + features.get(featureIndex) + "@"
                            + featureIndex;
                }
                System.err.println(out);
            }
        }
        else {
            System.err.println("Source " + ruleWritable.toString()
                    + " not found");
        }
    }
}
