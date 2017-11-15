package com.criteo.biggraphite.graphiteindex;

import org.apache.lucene.document.*;

import java.util.function.BiConsumer;
import java.util.regex.Pattern;

public class LuceneUtils
{
    public static final String ELEMENT_SEPARATOR = ".";
    public static final String ELEMENT_SEPARATOR_REGEXP = Pattern.quote(ELEMENT_SEPARATOR);

    public static final String FIELD_PATH = "path";
    public static final String FIELD_LENGTH = "length";
    public static final String FIELD_PART_PREFIX = "p";
    public static final String FIELD_OFFSET = "offset";

    public static Document toDocument(String path)
    {
        Document doc = new Document();

        int length = iterateOnElements(
            path,
            (element, depth) -> doc.add(
                new StringField(
                    FIELD_PART_PREFIX + depth,
                    element,
                    Field.Store.NO
                )
            )
        );

        doc.add(new StringField(FIELD_PATH, path, Field.Store.YES));
        doc.add(new IntPoint(FIELD_LENGTH, length));

        return doc;
    }

    public static Document toDocument(String path, long offset)
    {
        Document doc = toDocument(path);
        doc.add(new StoredField(FIELD_OFFSET, offset));

        return doc;
    }

    public static String getPathFromDocument(Document doc)
    {
        return doc.getField(FIELD_PATH).stringValue();
    }

    public static long getOffsetFromDocument(Document doc)
    {
        return doc.getField(FIELD_OFFSET).numericValue().longValue();
    }

    public static int iterateOnElements(String path, BiConsumer<String, Integer> f)
    {
        String[] elements = path.split(ELEMENT_SEPARATOR_REGEXP);
        for (int i = 0; i < elements.length; i++) {
            f.accept(elements[i], i);
        }

        return elements.length;
    }
}
