/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;
import uk.ac.cam.eng.extraction.hadoop.util.Util;

/**
 * @author jmp84 Utility to convert the output SequenceFile of the MapReduce
 *         feature merge job into an HFile. Note that there is only one input
 *         SequenceFile because there is only one reducer of the merge job so
 *         that we are sure that the SequenceFile output is sorted.
 */
public class SequenceFile2HFile {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Args: <input sequence file> <output hfile>");
            System.exit(1);
        }
        System.out.println("Reading " + args[0] + " and writing hfile to "
                + args[1]);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader sequenceReader =
                new SequenceFile.Reader(fs, new Path(args[0]), conf);
        Path path = new Path(args[1]);
        if (fs.exists(path)) {
            System.out.println("ERROR: " + args[1] + " already exists");
            System.exit(1);
        }
        HFile.WriterFactory hfileWriterFactory = HFile.getWriterFactory(conf);
        HFile.Writer hfileWriter =
                hfileWriterFactory
                        .createWriter(fs, path, 64 * 1024, "gz", null);
        BytesWritable key = new BytesWritable();
        ArrayWritable value = new ArrayWritable(GeneralPairWritable3.class);
        while (sequenceReader.next(key, value)) {
            byte[] valueBytes = Util.object2ByteArray(value);
            hfileWriter.append(key.getBytes(), valueBytes);
        }
        hfileWriter.close();
    }
}
