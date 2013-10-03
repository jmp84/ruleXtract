/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.util;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Temporary class for debugging, converts a sequence file into a
 *         text file
 */
public class SequenceFileToText {

    private static byte[] object2ByteArray(Writable obj) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        obj.write(out);
        return buffer.toByteArray();
    }

    private static RuleWritable convertValueBytes(byte[] inputBytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(inputBytes, 0, inputBytes.length);
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

    private static void convert(String inputFile, String outputFile)
            throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader sequenceReader = new SequenceFile.Reader(fs,
                new Path(inputFile), conf);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
            boolean stop = false;
            while (!stop) {
                RuleWritable key = new RuleWritable();
                MapWritable value = new MapWritable();
                if (sequenceReader.next(key, value)) {
                    bw.write(key.toString());
                    for (Writable featureIndex: value.keySet()) {
                        bw.write(" " + value.get(featureIndex) + "@"
                                + featureIndex);
                    }
                    bw.write("\n");
                }
                else {
                    stop = true;
                }
            }
        }
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        convert(args[0], args[1]);
    }

}
