/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable2;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84
 */
public class HFile2Text2 {

    private static ArrayWritable bytes2ArrayWritable(ByteBuffer bytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes.array(), bytes.arrayOffset(), bytes.limit());
        ArrayWritable value = new ArrayWritable(GeneralPairWritable2.class);
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

    private static RuleWritable bytes2RuleWritable(ByteBuffer bytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes.array(), bytes.arrayOffset(), bytes.limit());
        RuleWritable value = new RuleWritable();
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

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String hfileInput = args[0];
        String fileOutput = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        try (BufferedWriter bw =
                new BufferedWriter(new FileWriter(fileOutput))) {
            HFile.Reader hfileReader =
                    HFile.createReader(fs, new Path(hfileInput),
                            new CacheConfig(conf));
            hfileReader.loadFileInfo();
            HFileScanner scanner = hfileReader.getScanner(false, false, false);
            scanner.seekTo();
            do {
                RuleWritable key = bytes2RuleWritable(scanner.getKey());
                ArrayWritable value = bytes2ArrayWritable(scanner.getValue());
                for (int i = 0; i < value.get().length; i++) {
                    bw.write(key.getLeftHandSide()
                            + " "
                            + key.getSource()
                            + " "
                            + ((GeneralPairWritable2) value.get()[i])
                                    .getFirst()
                                    .getTarget());
                    MapWritable features =
                            ((GeneralPairWritable2) value.get()[i]).getSecond();
                    for (Writable featureIndex: features.keySet()) {
                        bw.write(" " + features.get(featureIndex) + "@"
                                + featureIndex);
                    }
                    bw.write("\n");
                }
            }
            while (scanner.next());
        }
    }
}
