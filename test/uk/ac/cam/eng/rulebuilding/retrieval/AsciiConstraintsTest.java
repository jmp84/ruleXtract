/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84
 */
public class AsciiConstraintsTest {

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

    @Test
    public void testGetAsciiConstraints() throws NumberFormatException,
            IOException {
        AsciiConstraints asciiConstraints = new AsciiConstraints();
        Set<Rule> rulesActual = asciiConstraints
                .getAsciiConstraints(System.getProperty("user.dir")
                        + "/test/uk/ac/cam/eng/rulebuilding/retrieval/asciiconstraints");
        Set<Rule> rulesExpected = new HashSet<Rule>();
        try (BufferedReader br = new BufferedReader(
                new FileReader(
                        System.getProperty("user.dir")
                                + "/test/uk/ac/cam/eng/rulebuilding/retrieval/asciirules"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+");
                String[] sourceString = parts[0].split("_");
                String[] targetString = parts[1].split("_");
                List<Integer> source = new ArrayList<Integer>();
                List<Integer> target = new ArrayList<Integer>();
                for (String ss: sourceString) {
                    source.add(Integer.parseInt(ss));
                }
                for (String ts: targetString) {
                    target.add(Integer.parseInt(ts));
                }
                Rule rule = new Rule(source, target);
                rulesExpected.add(rule);
            }
        }
        assertEquals(rulesExpected, rulesActual);
    }
}
