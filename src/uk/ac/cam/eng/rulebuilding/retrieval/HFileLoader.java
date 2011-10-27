/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;

/**
 * @author jmp84 This class takes a sorted file in HDFS and converts it into an
 *         HFile
 */
public class HFileLoader extends Configured {

    private static byte[] long2ByteArray(long l) {
        byte b[] = new byte[8];
        ByteBuffer buf = ByteBuffer.wrap(b);
        buf.putLong(l);
        return b;
    }

    private static byte[] object2ByteArray(Writable obj) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        obj.write(out);
        return buffer.toByteArray();
    }

    protected static void hdfs2HFile(String sortedInput, String hFile)
            throws IOException {
        // TODO replace this with a logger
        System.out.println("Reading " + sortedInput + " and writing hfile to "
                + hFile);
        Configuration conf = new Configuration();
        // Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        // FSDataInputStream in = fs.open(new Path(sortedInput));
        // LineRecordReader lineRecordReader = new LineRecordReader(in, 0,
        // Long.MAX_VALUE, conf);
        // LineRecordReader lineRecordReader = new LineRecordReader(in, 0,
        // Long.MAX_VALUE, Integer.MAX_VALUE);
        SequenceFile.Reader sequenceReader = new SequenceFile.Reader(fs,
                new Path(sortedInput), conf);
        Path path = new Path(hFile);
        if (fs.exists(path)) {
            System.out.println("Error: " + hFile + " already exists");
            System.exit(1);
        }
        HFile.Writer writer = new HFile.Writer(fs, path, 16 * 1024, "gz", null);
        BytesWritable key = new BytesWritable();
        ArrayWritable value = new ArrayWritable(PairWritable3.class);
        while (sequenceReader.next(key, value)) {
            byte[] valueBytes = object2ByteArray(value);
            writer.append(key.getBytes().clone(), 0, key.getLength(),
                    valueBytes, 0, valueBytes.length);
        }
        writer.close();
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Usage: HFileLoader sortedInput hFile");
            System.exit(1);
        }
        hdfs2HFile(args[0], args[1]);
    }

}
