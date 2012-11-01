package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.List;

public interface RandomReader<V> {

  V[] read(Object[] key) throws IOException;
}
