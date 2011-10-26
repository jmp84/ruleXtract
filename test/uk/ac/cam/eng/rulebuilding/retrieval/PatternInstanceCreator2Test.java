/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
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
public class PatternInstanceCreator2Test {

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
     * {@link uk.ac.cam.eng.rulebuilding.retrieval.PatternInstanceCreator22#createSourcePatterns(java.lang.String)}
     * .
     */
    @Test
    public void testCreateSourcePatterns() {
        PatternInstanceCreator2 pic = new PatternInstanceCreator2();
        List<SidePattern> actualPatterns = pic.createSourcePatterns(System
                .getProperty("user.dir")
                + "/test/uk/ac/cam/eng/rulebuilding/retrieval/patterns");
        String[] pattern1Array = {"w", "-1"};
        String[] pattern2Array = {"-1", "w"};
        String[] pattern3Array = {"w", "-1", "w"};
        SidePattern p1 = new SidePattern(Arrays.asList(pattern1Array));
        SidePattern p2 = new SidePattern(Arrays.asList(pattern2Array));
        SidePattern p3 = new SidePattern(Arrays.asList(pattern3Array));
        List<SidePattern> expectedPatterns = new ArrayList<SidePattern>();
        expectedPatterns.add(p1);
        expectedPatterns.add(p2);
        expectedPatterns.add(p3);
        assertEquals(expectedPatterns, actualPatterns);
    }

    /**
     * Test method for
     * {@link uk.ac.cam.eng.rulebuilding.retrieval.PatternInstanceCreator2#createSourcePatternInstances(java.util.Properties, java.util.List)}
     * .
     */
    // /*
    @Test
    public void testCreateSourcePatternInstances() {
        PatternInstanceCreator2 pic = new PatternInstanceCreator2();
        String testFile = System.getProperty("user.dir")
                + "/test/uk/ac/cam/eng/rulebuilding/retrieval/testfile";
        int maxLength = 5;
        List<SidePattern> sidePatterns = pic.createSourcePatterns(System
                .getProperty("user.dir")
                + "/test/uk/ac/cam/eng/rulebuilding/retrieval/patterns");
        Set<Rule> actualRules = pic.createSourcePatternInstances(testFile,
                sidePatterns);
        Set<Rule> expectedRules = new HashSet<Rule>();
        Integer[] r1 = {3};
        Integer[] r2 = {4};
        Integer[] r3 = {5};
        Integer[] r4 = {3, 4};
        Integer[] r5 = {4, 5};
        Integer[] r6 = {3, 4, 5};
        Integer[] r7 = {3, -1};
        Integer[] r8 = {4, -1};
        Integer[] r9 = {3, 4, -1};
        Integer[] r10 = {-1, 4};
        Integer[] r11 = {-1, 5};
        Integer[] r12 = {-1, 4, 5};
        Integer[] r13 = {3, -1, 5};
        expectedRules
                .add(new Rule(Arrays.asList(r1), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r2), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r3), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r4), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r5), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r6), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r7), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r8), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r9), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r10), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r11), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r12), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r13), new ArrayList<Integer>()));
        assertEquals(expectedRules, actualRules);
    }

    // */

    /**
     * Test method for
     * {@link uk.ac.cam.eng.rulebuilding.retrieval.PatternInstanceCreator2#getPatternInstancesFromSource(java.util.List, java.util.List)}
     * .
     */
    @Test
    public void testGetPatternInstancesFromSource() {
        PatternInstanceCreator2 pic = new PatternInstanceCreator2();
        List<SidePattern> sidePatterns = pic.createSourcePatterns(System
                .getProperty("user.dir")
                + "/test/uk/ac/cam/eng/rulebuilding/retrieval/patterns");
        Integer[] sourcePhraseArray = {3, 4};
        Set<Rule> actualRules = pic.getPatternInstancesFromSource(
                Arrays.asList(sourcePhraseArray), sidePatterns);
        Set<Rule> expectedRules = new HashSet<Rule>();
        Integer[] r1 = {3, 4};
        Integer[] r2 = {3, -1};
        Integer[] r3 = {-1, 4};
        expectedRules
                .add(new Rule(Arrays.asList(r1), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r2), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r3), new ArrayList<Integer>()));
        assertEquals(expectedRules, actualRules);
    }

    /**
     * Test method for
     * {@link uk.ac.cam.eng.rulebuilding.retrieval.PatternInstanceCreator2#merge(uk.ac.cam.eng.extraction.datatypes.Rule, java.util.Set)}
     * .
     */
    // /*
    @Test
    public void testMerge() {
        fail("Not yet implemented");
    }

    // */

    /**
     * Test method for
     * {@link uk.ac.cam.eng.rulebuilding.retrieval.PatternInstanceCreator2#getPatternInstancesFromSourceAndPattern(java.util.List, uk.ac.cam.eng.rulebuilding.retrieval.SidePattern, int, int)}
     * .
     */
    @Test
    public void testGetPatternInstancesFromSourceAndPattern() {
        PatternInstanceCreator2 pic = new PatternInstanceCreator2();
        Integer[] sourcePhraseArray = {3, 4, 5};
        // Integer[] sourcePhraseArray = {4, 5};
        List<Integer> sourcePhrase = Arrays.asList(sourcePhraseArray);
        String[] patternArray = {"w", "-1"};
        // String[] patternArray = {"-1"};
        SidePattern sidePattern = new SidePattern(Arrays.asList(patternArray));
        Set<Rule> actualRules = pic.getPatternInstancesFromSourceAndPattern(
                sourcePhrase, sidePattern, 0, 0);
        Integer[] r1 = {3, -1};
        Integer[] r2 = {3, 4, -1};
        // Integer[] r1 = {-1};
        Set<Rule> expectedRules = new HashSet<Rule>();
        expectedRules
                .add(new Rule(Arrays.asList(r1), new ArrayList<Integer>()));
        expectedRules
                .add(new Rule(Arrays.asList(r2), new ArrayList<Integer>()));
        // System.err.println(expectedRules);
        // System.err.println(actualRules);
        assertEquals(expectedRules, actualRules);
    }

    @Test
    public void testGetPatternInstancesFromSourceAndPattern2() {
        PatternInstanceCreator2 pic = new PatternInstanceCreator2();
        Integer[] sourcePhraseArray = {3, 4, 5, 6, 7};
        // Integer[] sourcePhraseArray = {4, 5};
        List<Integer> sourcePhrase = Arrays.asList(sourcePhraseArray);
        String[] patternArray = {"-3", "w", "-2", "w"};
        // String[] patternArray = {"-1"};
        SidePattern sidePattern = new SidePattern(Arrays.asList(patternArray));
        Set<Rule> actualRules = pic.getPatternInstancesFromSourceAndPattern(
                sourcePhrase, sidePattern, 0, 0);
        for (Rule r: actualRules) {
            System.err.println(r.toString());
        }
    }
}
