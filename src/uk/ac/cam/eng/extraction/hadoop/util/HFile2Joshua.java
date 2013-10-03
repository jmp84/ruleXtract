/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Utility to convert an HFile to text format as used by the
 *         Joshua decoder
 */
public class HFile2Joshua {

    private static String convert2Joshua(String rule) {
        String[] parts = rule.split("\\s+");
        if (parts.length != 3) {
            System.err.println("ERROR: malformed rule: " + rule);
        }
        String source = parts[1].replaceAll("_", " ");
        source = source.replaceAll("-1", "[X,1]");
        source = source.replaceAll("-2", "[X,1]");
        source = source.replaceAll("-3", "[X,2]");
        String target = parts[2].replaceAll("_", " ");
        target = target.replaceAll("-1", "[X,1]");
        target = target.replaceAll("-2", "[X,1]");
        target = target.replaceAll("-3", "[X,2]");
        return "[X] ||| " + source + " ||| " + target + " |||";
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Args: <input HFile> <output HFile>");
            System.exit(1);
        }
        String hfileInput = args[0];
        String fileOutput = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileOutput))) {
            HFile.Reader hfileReader =
                    HFile.createReader(fs, new Path(hfileInput),
                            new CacheConfig(conf));
            hfileReader.loadFileInfo();
            HFileScanner scanner = hfileReader.getScanner(false, false);
            scanner.seekTo();
            do {
                RuleWritable key = Util.bytes2RuleWritable(scanner.getKey());
                ArrayWritable value =
                        Util.bytes2ArrayWritable(scanner.getValue());
                for (int i = 0; i < value.get().length; i++) {
                    String rule =
                            key.getLeftHandSide()
                                    + " "
                                    + key.getSource()
                                    + " "
                                    + ((GeneralPairWritable3) value.get()[i])
                                            .getFirst().getTarget();
                    String ruleJoshua = convert2Joshua(rule);
                    bw.write(ruleJoshua);
                    AbstractMapWritable features =
                            ((GeneralPairWritable3) value.get()[i]).getSecond();
                    for (Writable featureIndex: ((SortedMapWritable) features)
                            .keySet()) {
                        Writable featureValueWritable =
                                ((SortedMapWritable) features)
                                        .get(featureIndex);
                        double featureValue = 0d;
                        if (featureValueWritable.getClass() == DoubleWritable.class) {
                            featureValue =
                                    ((DoubleWritable) ((SortedMapWritable) features)
                                            .get(featureIndex)).get();
                        }
                        else {
                            featureValue =
                                    ((IntWritable) ((SortedMapWritable) features)
                                            .get(featureIndex)).get();
                        }
                        if (Math.floor(featureValue) == featureValue) {
                            int featureValueInt = (int) featureValue;
                            bw.write(String.format(" %d", featureValueInt));
                        }
                        else {
                            bw.write(String.format(" %f", featureValue));
                        }
                    }
                    bw.write("\n");
                }
            }
            while (scanner.next());
        }
    }
}
