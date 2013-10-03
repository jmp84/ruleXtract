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
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.BlockIndexReader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

/**
 * @author jmp84 Utility by Rory Waite to create partitions from an hfile. The
 *         partitions can be reused in the mapreduce feature merging step so
 *         that the output is sorted prior to dumping to an hfile.
 */
public class CreatePartitions {

    private static final int PARTITIONS = 50;

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Args: <input hfile> <output partition file>");
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        CacheConfig cacheConfig = new CacheConfig(conf);
        HFile.Reader reader =
                HFile.createReader(fs, new Path(args[0]), cacheConfig);
        reader.loadFileInfo();
        SequenceFile.Writer writer =
                new SequenceFile.Writer(fs, conf, new Path(args[1]),
                        BytesWritable.class, NullWritable.class);
        BlockIndexReader blockReader = reader.getDataBlockIndexReader();
        int blockCount = blockReader.getRootBlockCount();
        int partitionSize = blockCount / PARTITIONS;
        BytesWritable key = new BytesWritable();
        for (int i = 1; i < PARTITIONS; i++) {
            byte[] keyBytes = blockReader.getRootBlockKey(i * partitionSize);
            key.set(keyBytes, 0, keyBytes.length);
            writer.append(key, NullWritable.get());
        }
        writer.close();
    }

}
