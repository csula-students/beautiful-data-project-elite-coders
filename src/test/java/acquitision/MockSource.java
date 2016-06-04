package acquitision;

import com.google.common.collect.Lists;

import dataAcquisition.model;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Consumer;

/**
 * A mock source to provide data
 */
public class MockSource{
    int index = 0;

    public boolean hasNext() {
        return index < 1;
    }

    public ArrayList<MockData> next() {
        return Lists.newArrayList(
            new MockData("6", null),
            new MockData("6", "3452"),
            new MockData("8", "1234")
        );
    }
}
