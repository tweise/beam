/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.apex.translators.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StandardCoder;

public interface ApexStreamTuple<T>
{
  /**
   * Gets the value of the tuple
   *
   * @return
   */
  T getValue();

  /**
   * Plain tuple class
   *
   * @param <T>
   */
  class DataTuple<T> implements ApexStreamTuple<T>
  {
    private T value;

    public static <T> DataTuple<T> of(T value) {
      return new DataTuple<>(value);
    }

    private DataTuple(T value)
    {
      this.value = value;
    }

    @Override
    public T getValue()
    {
      return value;
    }

    public void setValue(T value)
    {
      this.value = value;
    }

    @Override
    public String toString()
    {
      return value.toString();
    }

  }

  /**
   * Tuple that includes a timestamp
   *
   * @param <T>
   */
  class TimestampedTuple<T> extends DataTuple<T>
  {
    private long timestamp;

    public TimestampedTuple(long timestamp, T value)
    {
      super(value);
      this.timestamp = timestamp;
    }

    public long getTimestamp()
    {
      return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
      this.timestamp = timestamp;
    }
  }

  /**
   * Tuple that represents a watermark
   *
   * @param <T>
   */
  class WatermarkTuple<T> extends TimestampedTuple<T>
  {
    public static <T> WatermarkTuple<T> of(long timestamp) {
      return new WatermarkTuple<>(timestamp);
    }

    protected WatermarkTuple(long timestamp)
    {
      super(timestamp, null);
    }

    @Override
    public String toString()
    {
      return "[Watermark " + getTimestamp() + "]";
    }
  }

  /**
   * Coder for {@link ApexStreamTuple}.
   */
  public static class ApexStreamTupleCoder<T> extends StandardCoder<ApexStreamTuple<T>> {
    private static final long serialVersionUID = 1L;
    final Coder<T> valueCoder;

    public static <T> ApexStreamTupleCoder<T> of(Coder<T> valueCoder) {
      return new ApexStreamTupleCoder<>(valueCoder);
    }

    protected ApexStreamTupleCoder(Coder<T> valueCoder) {
      this.valueCoder = checkNotNull(valueCoder);
    }

    @Override
    public void encode(ApexStreamTuple<T> value, OutputStream outStream, Context context)
        throws CoderException, IOException {
      if (value instanceof WatermarkTuple) {
        outStream.write(1);
        new DataOutputStream(outStream).writeLong(((WatermarkTuple<?>)value).getTimestamp());
      } else {
        outStream.write(0);
        valueCoder.encode(value.getValue(), outStream, context);
      }
    }

    @Override
    public ApexStreamTuple<T> decode(InputStream inStream, Context context)
        throws CoderException, IOException
    {
      int b = inStream.read();
      if (b == 1) {
        return new WatermarkTuple<T>(new DataInputStream(inStream).readLong());
      } else {
        return new DataTuple<T>(valueCoder.decode(inStream, context));
      }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments()
    {
      return Arrays.<Coder<?>>asList(valueCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException
    {
      verifyDeterministic(
          this.getClass().getSimpleName() + " requires a deterministic valueCoder",
          valueCoder);
    }

    /**
     * Returns the value coder.
     */
    public Coder<T> getValueCoder() {
      return valueCoder;
    }

  }

}
