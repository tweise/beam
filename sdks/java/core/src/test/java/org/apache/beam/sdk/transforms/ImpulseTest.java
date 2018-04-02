package org.apache.beam.sdk.transforms;

import java.util.Arrays;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesImpulse;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test.
 */
@RunWith(JUnit4.class)
public class ImpulseTest {

  @Rule
  public transient TestPipeline p = TestPipeline.create();

  @Test
  @Category({ValidatesRunner.class, UsesImpulse.class})
  public void testImpulseRead() {
    PCollection<Integer> result = p.apply(Impulse.create())
        .apply(
            FlatMapElements.into(TypeDescriptors.integers())
                .via(input -> Arrays.asList(1, 2, 3)));
    PAssert.that(result).containsInAnyOrder(1, 2, 3);
    p.run().waitUntilFinish();
  }

}
