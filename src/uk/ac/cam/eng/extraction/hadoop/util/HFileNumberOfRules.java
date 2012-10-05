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

/**
 * @author jmp84 Counts the number of rules in an hfile
 */
public class HFileNumberOfRules {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String hfileInput = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        HFile.Reader hfileReader =
                HFile.createReader(fs, new Path(hfileInput),
                        new CacheConfig(conf));
        hfileReader.loadFileInfo();
        HFileScanner scanner = hfileReader.getScanner(false, false);
        scanner.seekTo();
        long numberOfRules = 0;
        do {
            ArrayWritable value =
                    Util.bytes2ArrayWritable(scanner.getValue());
            numberOfRules += value.get().length;
        }
        while (scanner.next());
        System.out.println("Number of rule: " + numberOfRules);
    }
}
