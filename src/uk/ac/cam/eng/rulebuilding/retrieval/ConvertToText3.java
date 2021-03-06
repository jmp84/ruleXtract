/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3ArrayWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Temporary class for debugging, converts a sequence file into a
 *         text file
 */
public class ConvertToText3 {

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
                BytesWritable key = new BytesWritable();
                // ArrayWritable value = new ArrayWritable(PairWritable3.class);
                PairWritable3ArrayWritable value =
                        new PairWritable3ArrayWritable();
                if (sequenceReader.next(key, value)) {
                    RuleWritable source = convertValueBytes(key.getBytes());
                    for (int i = 0; i < value.get().length; i++) {
                        bw.write(source.getLeftHandSide() + " "
                                + source.getSource() + " "
                                // + source.getTarget() + " "
                                + value.get()[i].first.getTarget());
                        // + value.get()[i].first.getSource() + "\n");
                        Writable[] features = value.get()[i].second.get();
                        for (int j = 0; j < features.length; j++) {
                            bw.write(" " + ((DoubleWritable) features[j]).get());
                        }
                        bw.write("\n");
                    }
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
