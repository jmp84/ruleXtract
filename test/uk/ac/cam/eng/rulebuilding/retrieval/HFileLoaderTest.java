/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author jmp84
 */
public class HFileLoaderTest {

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {}

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {}

    /**
     * Test method for
     * {@link uk.ac.cam.eng.rulebuilding.retrieval.HFileLoader#hdfs2HFile(java.lang.String, java.lang.String)}
     * .
     * 
     * @throws IOException
     */
    @Test
    public void testHdfs2HFile() throws IOException {
        StringBuilder fileContentExpected = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                getClass().getResourceAsStream("testdata")))) {
            String line;
            while ((line = br.readLine()) != null) {
                fileContentExpected.append(line + "\n");
            }
            File hfileOut = new File("/tmp/hfile.gz");
            if (hfileOut.exists()) {
                hfileOut.delete();
            }
            HFileLoader.hdfs2HFile(System.getProperty("user.dir")
                    + "/test/uk/ac/cam/eng/rulebuilding/retrieval/testdata",
                    "/tmp/hfile.gz");
            StringBuilder fileContentActual = new StringBuilder();
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            HFile.Reader hfileReader = new HFile.Reader(fs, new Path(
                    "/tmp/hfile.gz"), null, false);
            hfileReader.loadFileInfo();
            HFileScanner hfileScanner = hfileReader.getScanner();
            hfileScanner.seekTo();
            do {
                // System.err.println(hfileScanner.getValueString());
                fileContentActual.append(hfileScanner.getValueString() + "\n");
            }
            while (hfileScanner.next());
            assertEquals(fileContentExpected.toString(),
                    fileContentActual.toString());
        }
    }
}
