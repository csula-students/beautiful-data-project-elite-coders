package acquitision;

import com.google.common.collect.Lists;

import dataAcquisition.csv;
import dataAcquisition.model;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

/**
 * A test case to show how to use Collector and Source
 */
public class CollectorTest {
    private ArrayList<model> collector;
    private MockSource source;

    @Before
    public void setup() throws IOException {
    	dataAcquisition.csv csv=new csv();
        collector = csv.getlist();
        source = new MockSource();
    }

    @Test
    public void mungee() throws Exception {
        List<SimpleModel> list = (List<SimpleModel>) source;
        List<SimpleModel> expectedList = Lists.newArrayList(
            new SimpleModel("2", "content2"),
            new SimpleModel("3", "content3")
        );
        Assert.assertEquals(list.size(), 2);

        for (int i = 0; i < 2; i ++) {
            Assert.assertEquals(list.get(i).getMovieid(), expectedList.get(i).getMovieid());
            Assert.assertEquals(list.get(i).getRatings(), expectedList.get(i).getRatings());
        }
    }
}