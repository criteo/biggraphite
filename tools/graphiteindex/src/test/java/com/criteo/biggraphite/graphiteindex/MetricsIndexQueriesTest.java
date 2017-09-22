package com.criteo.biggraphite.graphiteindex;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests queries against an in-memory metrics index.
 * Offset values are generated using the path's hashCode.
 */
@RunWith(Parameterized.class)
public class MetricsIndexQueriesTest {
    private static Set<String> setOf(String... xs) {
        return new HashSet<>(Arrays.asList(xs));
    }

    public static final Object[][] testQueries = {
        {
            "a.single.metric",
            setOf(
                "a.single.metric"
            ),
        },
        {
            "a.few.metrics.*",
            setOf(
                "a.few.metrics.a",
                "a.few.metrics.b",
                "a.few.metrics.c"
            ),
        },
        {
            "metrics.match{ed_by,ing}.s[ao]me.regexp.?[0-9]",
            setOf(
                "metrics.matched_by.same.regexp.b2",
                "metrics.matched_by.some.regexp.a1",
                "metrics.matching.same.regexp.w5",
                "metrics.matching.some.regexp.z8"
            ),
        },
    };

    public static final String[] unmatchedMetricNames = {
        "a.single.metric.two",

        "a.few.metrcs.a",
        "a.few.metrocs.z",

        "metrics.matxing.some.regexp.a1",
        "metrics.matchacha.same.regexp.z5"
    };

    public static MetricsIndex index;

    @BeforeClass
    @SuppressWarnings("unchecked")
    public static void buildTestIndex() throws IOException {
        index = new MetricsIndex("test-index");

        for (Object[] testQuery : testQueries) {
            Set<String> metricNames = (Set<String>)testQuery[1];
            for (String metricName : metricNames) {
                index.insert(metricName, (long)metricName.hashCode());
            }
        }

        for (String metricName : unmatchedMetricNames) {
            index.insert(metricName, (long)metricName.hashCode());
        }

        index.forceCommit();
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(testQueries);
    }

    @Parameter(0)
    public String query;

    @Parameter(1)
    public Set<String> expectedResults;

    @Test
    public void shouldReturnCorrectSearchResults() {
        List<Pair<String, Long>> results = index.searchPaths(query);
        Set<String> uniquePaths = results
            .stream()
            .map(Pair::getLeft)
            .collect(Collectors.toSet());

        assertEquals(expectedResults, uniquePaths);
    }

    @Test
    public void shouldStoreAndRetrieveOffsetValues() {
        for (Pair<String, Long> entry : index.searchPaths(query)) {
            assertEquals((long)entry.getLeft().hashCode(), entry.getRight().longValue());
        }
    }
}
