package org.apache.hadoop.hive.ql.io;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.util.IndexedSortable;

public class SortedStorage implements IndexedSortable {

  private static final byte SEP = '\t';
  private static final byte CR = '\r';
  private static final byte LF = '\n';

  int sorting = 1;
  byte separator;

  byte[] data;
  int[][][] index;        // count:column:offset/length/recordID
//    List<int[]>[] swapping;     // column:count:swap-a/swap-b

  SortedStorage(byte[] data, int columns) {
    this(data, columns, SEP);
  }

  SortedStorage(byte[] data, int columns, byte separator) {
    this.data = data;
    this.separator = separator;
//        this.swapping = swappings(columns);
    this.index = indexing(data, columns);
  }

  @SuppressWarnings("unchecked")
  private List<int[]>[] swappings(int columns) {
    List<int[]>[] swapping = new ArrayList[columns];
    for (int i = 0; i < columns; i++) {
      swapping[i] = new ArrayList<int[]>();
    }
    return swapping;
  }

  private int[][][] indexing(byte[] data, int columns) {
    List<int[][]> list = new ArrayList<int[][]>();
    LineReader reader = new LineReader(data, columns, separator);

    int counter = 0;
    int[][] indexed;
    while ((indexed = reader.nextLine(counter++)) != null) {
      list.add(indexed);
    }
    return list.toArray(new int[list.size()][][]);
  }

//    public int[] shuffle(int column1, int column2) {
//        int[] shuffle = new int[index.length];
//        for (int i = 0; i < shuffle.length; i++) {
//            shuffle[i] = i;
//        }
//        List<int[]> apply1 = swapping[column1 - 1];
//        List<int[]> apply2 = swapping[column2 - 1];
//        for (int i = apply1.size() - 1; i >=  0; i--) {
//            int[] swap = apply1.get(i);
//            int value = shuffle[swap[0]];
//            shuffle[swap[0]] = shuffle[swap[1]];
//            shuffle[swap[1]] = value;
//        }
//        for (int i = 0; i < apply2.size(); i++) {
//            int[] swap = apply2.get(i);
//            int value = shuffle[swap[0]];
//            shuffle[swap[0]] = shuffle[swap[1]];
//            shuffle[swap[1]] = value;
//        }
//        return shuffle;
//    }

//    public int[] shuffleX(int column1, int column2) {
//        int[] shuffle = new int[index.length - 1];
//        for (int i = 0; i < shuffle.length; i++) {
//            int rindex1 = index[i][column1][2];
//            int j = 0;
//            for (; j < shuffle.length; j++) {
//                if (rindex1 == index[j][column2][2]) {
//                    break;
//                }
//            }
//            shuffle[i] = j;
//        }
//        return shuffle;
//    }

  public int compare(int i, int j) {
    int[] index1 = index[i][sorting];
    int[] index2 = index[j][sorting];
    int compareLen = Math.min(index1[1], index2[1]);
    for (int k = 0; k < compareLen; k++) {
      int compared = data[index1[0] + k] - data[index2[0] + k];
      if (compared != 0) {
        return compared;
      }
    }
    return index1[1] - index2[1];
  }

  public void swap(int i, int j) {
//        swapping[sorting - 1].add(new int[]{i, j});
    int[] index1 = index[i][sorting];
    index[i][sorting] = index[j][sorting];
    index[j][sorting] = index1;
  }

  public static Iterator<byte[]> iterate(File directory, final int column) throws Exception {
    final RandomAccessFile raf = new RandomAccessFile(new File(directory, ".data"), "r");

    File ifile = new File(directory, column + ".ix");
    FileInputStream finput = new FileInputStream(ifile);

    final FileChannel channel = finput.getChannel();
    final MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, ifile.length());

    return new Iterator<byte[]>() {
      public boolean hasNext() {
        return buffer.remaining() > 12;
      }

      public byte[] next() {
        int offset = buffer.getInt();
        int length = buffer.getInt();
        int record = buffer.getInt();

        byte[] value = new byte[length];
        try {
          raf.seek(offset);
          raf.read(value);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        return value;
      }

      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public static Iterator<byte[]> iterate2(File directory, final int column, final int[] shuffle) throws Exception {
    final RandomAccessFile draf = new RandomAccessFile(new File(directory, ".data"), "r");
    File ifile = new File(directory, column + ".ix");
    FileInputStream finput = new FileInputStream(ifile);

    final FileChannel channel = finput.getChannel();
    final MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, ifile.length());

    return new Iterator<byte[]>() {

      int current;

      public boolean hasNext() {
        return current < shuffle.length;
      }

      public byte[] next() {
        try {
          buffer.position(shuffle[current] * 12);
          int offset = buffer.getInt();
          int length = buffer.getInt();
          int record = buffer.getInt();

          byte[] value = new byte[length];
          draf.seek(offset);
          draf.read(value);
          return value;
        } catch (IOException e) {
          throw new IllegalStateException(e);
        } finally {
          current++;
        }
      }

      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public Iterator<byte[]> iterate(final int column) {
    return new Iterator<byte[]>() {
      int cursor;

      public boolean hasNext() {
        return cursor < index.length;
      }

      public byte[] next() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutputStream doutput = new DataOutputStream(output);
        try {
          int offset = index[cursor][column][0];
          int length = index[cursor][column][1];
          int record = index[cursor][column][2];
//                    doutput.writeInt(length);
          doutput.write(data, offset, length);
          byte[] value = output.toByteArray();
//                    System.out.println("[LocalMapOutput/next] " + record +  " --> " + new String(value));
          return value;
        } catch (IOException e) {
          throw new IllegalStateException(e);
        } finally {
          cursor++;
        }
      }

      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public Iterator<byte[]> iterate2(final int from, final int to, final int[] shuffle) throws Exception {

    return new Iterator<byte[]>() {
      int cursor;

      public boolean hasNext() {
        return cursor < index.length;
      }

      public byte[] next() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutputStream doutput = new DataOutputStream(output);
        int source = index[cursor][from][2];
        int target = shuffle[source];
//                System.out.println("[LocalMapOutput/next] " + source + " --> " + target);
//                int target = source;
        try {
          int offset = index[target][from][0];
          int length = index[target][from][1];
          int record = index[target][from][2];
//                    doutput.writeInt(length);
          doutput.write(data, offset, length);
          byte[] value = output.toByteArray();
//                    System.out.println("[LocalMapOutput/next] " + record +  " --> " + new String(value));
          return value;
        } catch (IOException e) {
          throw new IllegalStateException(e);
        } finally {
          cursor++;
        }
      }

      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public String toString(int column) {
    StringBuilder builder = new StringBuilder();
    for (int[][] aIndex : index) {
      builder.append(new String(data, aIndex[column][0], aIndex[column][1]));
      builder.append('\n');
    }
    return builder.toString();
  }

  public void saveAll(File directory) throws Exception {
    if (!directory.exists() && !directory.mkdirs()) {
      throw new IllegalStateException("failed to create directory " + directory);
    }
    File dataf = new File(directory, ".data");
    FileOutputStream doutput = new FileOutputStream(dataf);
    doutput.write(data);
    doutput.close();

    File rindexf = new File(directory, ".ix");
    DataOutputStream routput = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(rindexf)));
    for (int i = 0; i < index.length; i++) {
      routput.writeInt(index[i][0][0]);
      routput.writeInt(index[i][0][1]);
    }
    routput.close();

    DataOutputStream[] indexed = new DataOutputStream[index[0].length - 1];
    DataOutputStream[] rindexed = new DataOutputStream[index[0].length - 1];
    for (int i = 1; i <= indexed.length; i++) {
      File cindexf = new File(directory, i + ".ix");
      indexed[i - 1] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(cindexf)));

      File crindexf = new File(directory, i + ".ixr");
      rindexed[i - 1] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(crindexf)));
    }
    for (int i = 0; i < index.length - 1; i++) {
      int[][] aIndex = index[i];
      for (int j = 1; j <= indexed.length; j++) {
        indexed[j - 1].writeInt(aIndex[j][0]);
        indexed[j - 1].writeInt(aIndex[j][1]);
        indexed[j - 1].writeInt(aIndex[j][2]);
      }
    }
    for (DataOutputStream output : indexed) {
      output.close();
    }
  }

  static class LineReader {

    byte separator;

    private byte[] data;
    private int columns;
    private int position;

    LineReader(byte[] data, int columns) {
      this(data, columns, SEP);
    }

    LineReader(byte[] data, int columns, byte separator) {
      this.data = data;
      this.columns = columns;
      this.separator = separator;
    }

    public int[][] nextLine(int rindex) {
      if (position >= data.length) {
        return null;
      }
      int column = 1;
      int[][] indexes = new int[columns + 1][];

      byte prevChar = 0;
      int rstart = position;
      int cstart = position;
      for (; position < data.length; prevChar = data[position++]) {
        if (data[position] == separator) {
          indexes[column++] = new int[]{cstart, (position - cstart), rindex};
          cstart = position + 1;
          continue;
        }
        if (data[position] == LF) {
          if (column < columns) {
            rstart = cstart = position + 1;
            continue;
          }
          int spliter = prevChar == CR ? 1 : 0;
          int length = position - cstart - spliter;
          if (length == 0) {
            indexes[0] = new int[]{rstart, (position - rstart - spliter)};
            return indexes;
          }
          indexes[column] = new int[]{cstart, length, rindex};
          indexes[0] = new int[]{rstart, (position - rstart - spliter)};
          position++;
          return indexes;
        }
      }
      if (column < columns) {
        return null;
      }
      int spliter = prevChar == CR ? 1 : 0;
      indexes[column] = new int[]{cstart, (position - cstart - spliter), rindex};
      indexes[0] = new int[]{rstart, (position - rstart - spliter)};
      position++;
      return indexes;
    }
  }
}