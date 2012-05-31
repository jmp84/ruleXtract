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
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84
 */
public class HFile2Text2 {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
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
                    bw.write(key.getLeftHandSide()
                            + " "
                            + key.getSource()
                            + " "
                            + ((GeneralPairWritable3) value.get()[i])
                                    .getFirst().getTarget());
                    AbstractMapWritable features =
                            ((GeneralPairWritable3) value.get()[i]).getSecond();
                    for (Writable featureIndex: ((SortedMapWritable) features)
                            .keySet()) {
                        bw.write(" "
                                + ((SortedMapWritable) features)
                                        .get(featureIndex) + "@" + featureIndex);
                    }
                    bw.write("\n");
                }
            }
            while (scanner.next());
        }
    }
}
