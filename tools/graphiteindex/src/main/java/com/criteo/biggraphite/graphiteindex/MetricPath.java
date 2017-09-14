package com.criteo.biggraphite.graphiteindex;

import java.util.function.BiConsumer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StringField;

public class MetricPath {
    public static final char ELEMENT_SEPARATOR = '.';

    public static final String FIELD_PATH = "path";
    public static final String FIELD_LENGTH = "length";
    public static final String FIELD_PART_PREFIX = "p";

    public static Document toDocument(String path) {
        Document doc = new Document();

        int length = iterateOnElements(
            path,
            (element, depth) -> doc.add(
                new StringField(
                    MetricPath.FIELD_PART_PREFIX + depth,
                    element,
                    Field.Store.NO
                )
            )
        );

        doc.add(new StringField(MetricPath.FIELD_PATH, path, Field.Store.YES));
        doc.add(new IntPoint(MetricPath.FIELD_LENGTH, length));

        return doc;
    }

    public static String fromDocument(Document doc) {
        return doc.getField(FIELD_PATH).stringValue();
    }

    public static int iterateOnElements(String path, BiConsumer<String, Integer> f) {
        int depth = 0;
        int start = 0;
        int end = path.indexOf(ELEMENT_SEPARATOR, start);
        do {
            if (end == -1) {
                end = path.length();
            }

            f.accept(path.substring(start, end), depth);

            depth += 1;
            start = end + 1;
            end = path.indexOf(ELEMENT_SEPARATOR, start);
        } while (start < path.length());

        return depth;
    }
}
