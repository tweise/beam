package org.apache.beam.sdk.transforms;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.core.construction.JavaReadViaImpulse;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesImpulse;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ImpulseTest {
  @Rule
  public transient TestPipeline p = TestPipeline.create();

  @Test
  @Category({ValidatesRunner.class, UsesImpulse.class})
  public void testImpulseRead() {
    PCollection<Integer> result = p.apply(JavaReadViaImpulse.bounded(Source.of(1, 2, 3)));
    PAssert.that(result).containsInAnyOrder(1, 2, 3);
    p.run().waitUntilFinish();
  }

  private static class Source extends BoundedSource<Integer> {

    private final List<Integer> elems;

    private Source(List<Integer> elems) {
      this.elems = elems;
    }

    static BoundedSource<Integer> of(int elem, int... elems) {
      ImmutableList.Builder<Integer> builder = ImmutableList.builder();
      builder.add(elem);
      if (elems != null) {
        for (int e : elems) {
          builder.add(e);
        }
      }
      return new Source(builder.build());
    }

    @Override
    public List<? extends BoundedSource<Integer>> split(long desiredBundleSizeBytes,
        PipelineOptions options) {
      return Collections.singletonList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      return 1;
    }

    @Override
    public BoundedReader<Integer> createReader(PipelineOptions options) {
      return new BoundedReader<Integer>() {
        Iterator<Integer> iterator = elems.iterator();
        int current = 0;

        @Override
        public BoundedSource<Integer> getCurrentSource() {
          return Source.this;
        }

        @Override
        public boolean start() {
          if (!iterator.hasNext()) {
            return false;
          }
          current = iterator.next();
          return true;
        }

        @Override
        public boolean advance() {
          if (!iterator.hasNext()) {
            return false;
          }
          current = iterator.next();
          return true;
        }

        @Override
        public Integer getCurrent() throws NoSuchElementException {
          return current;
        }

        @Override
        public void close() {}
      };
    }

    @Override
    public Coder<Integer> getOutputCoder() {
      return VarIntCoder.of();
    }
  }
}
