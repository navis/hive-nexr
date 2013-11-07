package org.apache.hive.service.cli;

import java.util.BitSet;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class TestColumnValue {

  @Test
  public void testConvertMaks() throws Exception {
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < 32000; i++) {
      BitSet bitset = new BitSet(random.nextInt(1024) + 512);
      for (int j = 0; j < random.nextInt(256) + 128; j++) {
        bitset.set(random.nextInt(1024));
      }
      BitSet converted = ColumnValue.toBitset(ColumnValue.toBinary(bitset));
      Assert.assertEquals(bitset, converted);
    }
  }
}
