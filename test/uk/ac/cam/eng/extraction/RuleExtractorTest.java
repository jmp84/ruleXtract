/**
 * 
 */

package uk.ac.cam.eng.extraction;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.cam.eng.extraction.datatypes.Alignment;
import uk.ac.cam.eng.extraction.datatypes.Block;
import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.datatypes.SentencePair;

/**
 * @author jmp84
 */
public class RuleExtractorTest {

    private SentencePair sp;
    private RuleExtractor r;
    private Alignment a;

    private String readFileContent(String fileName) throws IOException {
        StringBuilder fileContent = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                getClass().getResourceAsStream(fileName)))) {
            String line;
            while ((line = br.readLine()) != null) {
                fileContent.append(line);
            }
        }
        return fileContent.toString();
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        // TODO make lines short
        String acnAlign = "S 0 2\nS 3 3\nS 4 5\nS 5 6\nS 6 6\nS 7 7\nS 8 8\nS 9 9\nS 10 10\nS 11 11\nS 12 12\nS 13 13";
        String sentencePairString = "16 12 37 9 966 24 3 568 10 13 23 366 4806 5\n8 60 563 21 9 391 12 569 13 16 78 209 4590 5";
        sp = new SentencePair(sentencePairString, false);
        r = new RuleExtractor();
        a = new Alignment(acnAlign, sp, false);
    }

    /**
     * @throws java.lang.Exception
     */
    // @After
    // public void tearDown() throws Exception {}

    /**
     * Test method for
     * {@link uk.ac.cam.eng.extraction.RuleExtractor#RuleExtractor(org.apache.hadoop.conf.Configuration)}
     * .
     */
    @Test
    public void testRuleExtractorConfiguration() {
        String configFile = "example/exampleConfig";
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(configFile));
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Configuration conf = new Configuration();// = getConf();
        for (String prop: p.stringPropertyNames()) {
            conf.set(prop, p.getProperty(prop));
        }
        RuleExtractor r = new RuleExtractor(conf);
        assertEquals(5, r.MAX_SOURCE_PHRASE);
        assertEquals(5, r.MAX_SOURCE_ELEMENTS);
        assertEquals(5, r.MAX_TERMINAL_LENGTH);
        assertEquals(10, r.MAX_NONTERMINAL_LENGTH);
    }

    /**
     * Test method for
     * {@link uk.ac.cam.eng.extraction.RuleExtractor#extract(uk.ac.cam.eng.extraction.datatypes.Alignment, uk.ac.cam.eng.extraction.datatypes.SentencePair)}
     * .
     */
    @Test
    public void testExtract() {
        List<Rule> listRules = r.extract(a, sp);
        StringBuilder rules = new StringBuilder();
        for (Rule rule: listRules) {
            rules.append(rule.toString());
        }
        String fileContent = readFileContent("testdata/outputRuleExtractor");
        assertEquals(fileContent, rules.toString());
    }

    /**
     * Test method for
     * {@link uk.ac.cam.eng.extraction.RuleExtractor#extractRulesOneNonTerminal(int, int, int, int, uk.ac.cam.eng.extraction.datatypes.Alignment, uk.ac.cam.eng.extraction.datatypes.SentencePair)}
     * .
     */
    @Test
    public void testExtractRulesOneNonTerminal() {
        List<Rule> listRules = r.extractRulesOneNonTerminal(3, 7, 3, 7, a, sp);
        StringBuilder rules = new StringBuilder();
        for (Rule rule: listRules) {
            rules.append(rule.toString());
        }
        String fileContent = readFileContent("testdata/outputRuleExtractorOneNonTerminal");
        assertEquals(fileContent, rules.toString());
    }

    /**
     * Test method for
     * {@link uk.ac.cam.eng.extraction.RuleExtractor#extractRulesTwoNonTerminal(int, int, int, int, uk.ac.cam.eng.extraction.datatypes.Alignment, uk.ac.cam.eng.extraction.datatypes.SentencePair)}
     * .
     */
    @Test
    public void testExtractRulesTwoNonTerminal() {
        List<Rule> listRules = r
                .extractRulesTwoNonTerminal(3, 10, 3, 10, a, sp);
        StringBuilder rules = new StringBuilder();
        for (Rule rule: listRules) {
            rules.append(rule.toString());
        }
        String fileContent = readFileContent("testdata/outputRuleExtractorTwoNonTerminal");
        assertEquals(fileContent, rules.toString());
    }

    /**
     * Test method for
     * {@link uk.ac.cam.eng.extraction.RuleExtractor#extractPhrasePairs(uk.ac.cam.eng.extraction.datatypes.Alignment, uk.ac.cam.eng.extraction.datatypes.SentencePair)}
     * .
     */
    @Test
    public void testExtractPhrasePairs() {
        List<Rule> listRules = r.extractPhrasePairs(a, sp);
        StringBuilder rules = new StringBuilder();
        for (Rule rule: listRules) {
            rules.append(rule.toString());
        }
        String fileContent = readFileContent("testdata/outputRuleExtractorPhrasePairs");
        assertEquals(fileContent, rules.toString());
    }

    /**
     * Test method for
     * {@link uk.ac.cam.eng.extraction.RuleExtractor#extractHieroRules(uk.ac.cam.eng.extraction.datatypes.Alignment, uk.ac.cam.eng.extraction.datatypes.SentencePair)}
     * .
     */
    @Test
    public void testExtractHieroRules() {
        List<Rule> listRules = r.extractHieroRule(a, sp);
        StringBuilder rules = new StringBuilder();
        for (Rule rule: listRules) {
            rules.append(rule.toString());
        }
        String fileContent = readFileContent("testdata/outputRuleExtractorHieroRules");
        assertEquals(fileContent, rules.toString());
    }

    /**
     * Test method for
     * {@link uk.ac.cam.eng.extraction.RuleExtractor#extractHieroRules(uk.ac.cam.eng.extraction.datatypes.Alignment, uk.ac.cam.eng.extraction.datatypes.SentencePair)}
     * .
     */
    @Test
    public void testExtractRegularBlocks() {
        List<Block> listBlocks = r.getRegularBlocks(a, sp);
        StringBuilder blocks = new StringBuilder();
        for (Block b: listBlocks) {
            blocks.append(b.toString());
        }
        String fileContent = readFileContent("testdata/blocks");
        assertEquals(fileContent, blocks.toString());
    }
}
