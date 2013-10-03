/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

import uk.ac.cam.eng.extraction.hadoop.features.MapReduceFeatureMergeJob;

/**
 * @author jmp84 Utility to convert an HFile to a SequenceFile containing the
 *         keys (the values are thrown away). The file obtained may be used for
 *         the {@link TotalOrderPartitioner} in {@link MapReduceFeatureMergeJob}
 *         to have multiple reducers and keep the data sorted.
 */
public class HFile2SequenceFile {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Args: <input hfile> <output sequence file>");
            System.exit(1);
        }
        String hfileInput = args[0];
        String sequenceFileOutput = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Writer writer =
                new SequenceFile.Writer(fs, conf, new Path(sequenceFileOutput),
                        BytesWritable.class, NullWritable.class);
        HFile.Reader hfileReader = HFile.createReader(fs, new Path(hfileInput),
                new CacheConfig(conf));
        hfileReader.loadFileInfo();
        HFileScanner scanner = hfileReader.getScanner(false, false);
        scanner.seekTo();
        do {
            ByteBuffer source = scanner.getKey();
            BytesWritable sourceWritable = Util.bytes2BytesWritable(source);
            writer.append(sourceWritable, NullWritable.get());
        }
        while (scanner.next());
        writer.close();
    }
}
