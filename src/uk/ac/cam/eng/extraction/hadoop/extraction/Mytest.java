/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;

/**
 * @author jmp84
 */
public class Mytest extends TestCase {

    private ExtractorMapperMethod3 mapper;
    private ExtractorReducerMethod3 reducer;
    private MapReduceDriver driver;

    @Before
    public void setUp() {
        mapper = new ExtractorMapperMethod3();
        reducer = new ExtractorReducerMethod3();
        driver = new MapReduceDriver();
    }

    @Test
    public void testExtractorMapperMethod3() throws IOException {
        final IntWritable key = new IntWritable(1);
        final TextArrayWritable value = new TextArrayWritable();
        Text[] arrayValue = new Text[2];
        value.set(arrayValue);

        String sentenceAlign = "15 2623 2935 3 1709 6 3 28 50 4581 17 2874 1779 873 902 4 8 15 35 65 331 251 7 287 48 11 1303 81 182 9 3 196 10 48 3657 11 6421 18722 493 5\n"
                + "4991 7711 6 558 3 1391 143006 38 52 4 4796 6 2810 1836 3 888 307 4 11 6873 10 44 215 66 445 3 8 783 488 376 1245 4668 7";

        String wordAlign = "S 0 0\nS 1 0\nS 2 1\nS 3 2\nS 4 3\nS 4 5\nS 5 4\nS 5 6\nS 6 6\nS 7 7\nS 8 8\nS 9 10\nS 10 11\nS 10 12\n"
                + "S 11 12\nS 12 13\nS 13 15\nS 13 16\nS 14 16\nS 15 17\nS 16 18\nS 17 23\nS 18 19\nS 19 19\nS 20 19\nS 21 19\n"
                + "S 22 20\nS 23 24\nS 24 21\nS 24 22\nS 26 1\nS 27 1\nS 28 16\nS 31 24\nS 32 26\nS 33 26\nS 33 27\nS 34 28\nS 35 29\nS 36 30\nS 37 31\nS 38 31\nS 39 32";

        arrayValue[0] = new Text(sentenceAlign);
        arrayValue[1] = new Text(wordAlign);

        driver.run();

        // driver.withInput(new Text("foo"), new Text("bar"))
        // .withOutput(new Text("foo"), new Text("bar"))
        // .runTest();
    }
}
