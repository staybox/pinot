/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.realtime.impl.dictionary;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import javax.annotation.Nonnull;


/**
 * Off-heap mutable dictionaries have the following elements:
 * - A forward map from dictionary ID to the actual value.
 * - A reverse map from the value to the dictionary ID.
 *
 * This base class provides the reverse map functionality. The reverse map is realized using a list of off-heap
 * IntBuffers directly allocated.
 *
 * An on-heap overflow hashmap holds items that have hash collisions, until the overflow hashmap reaches a threshold
 * size. At this point, we add a new IntBuffer, and transfer items from the overflow hashmap into the newly allocated
 * buffer, and also create a new overflow hashmap to handle future collisions.
 *
 * The overflow hashmap is set to have a size threshold of OVERFLOW_THRESHOLD_PERCENT (as a percentage of the initial
 * estimated cardinality).
 *
 * To start with, we only have the on-heap overflow buffer. The IntBuffers are allocated when overflow hashmap reaches
 * a threshold number of entries.
 *
 * A buffer has N (a power of 2) rows  and NUM_COLUMNS columns, as below.
 * - The actual value for  NUM_COLUMNS is yet to be tuned.
 * - Each cell in the buffer can hold one integer.
 *
 *                | col 0 | col 1 | ..... | col M-1 |
 *        ==========================================|
 *        row 0   |       |       |       |         |
 *        ==========================================|
 *        row 1   |       |       |       |
 *        ==========================================|
 *          .
 *          .
 *          .
 *        ==========================================|
 *        row N-1 |       |       |       |         |
 *        ==========================================|
 *
 * To start with, all cells are initialized to have NULL_VALUE_INDEX (indicating empty cell)
 * Here is the pseudo-code for indexing an item or finding the dictionary ID of an item.
 *
 * index(item) {
 *   foreach (iBuf: iBufList) {
 *     hash value into a row for the buffer.
 *     foreach (cell: rowOfCells) {
 *       if (cell is not occupied) {
 *         set it to dictId
 *         return
 *       } else if (item.equals(get(dictId)) {
 *         // item already present in dictionary
 *         return
 *       }
 *     }
 *   }
 *   oveflow.put(item, dictId)
 *   if (overflow.size() > threshold) {
 *     newSize = lastBufSize * expansionMultiple
 *     newBuf = allocateDirect(newSize)
 *     add newBuf to iBufList
 *     newOverflow = new HashMap()
 *     foreach (entry: overflow) {
 *       hash entry.key() into a row for newBuf
 *       foreach (cell : rowOfCells) {
 *         if (cell empty) {
 *           set cell to entry.value();
 *         }
 *       }
 *       if (we did not enter value above) {
 *         newOverflow.put(entry.key(), entry.value());
 *       }
 *     }
 *   }
 * }
 *
 * indexOf(item) {
 *   foreach (iBuf: iBufList) {
 *     hash value into a row for the buffer;
 *     foreach (cell : rowOfCells) {
 *       if (cell is not occupied) {
 *         return NULL_VALUE_INDEX;
 *       }
 *       if (cell is occupied && item.equals(get(dictId))) {
 *         return dictId;
 *       }
 *     }
 *   }
 *   if (overflow.contains(item)) {
 *     return overflow.get(item);
 *   }
 *   return NULL_VALUE_INDEX;
 * }
 *
 * The list of buffers and the overflow hash are maintained in a class (ValueToDictId) that is referenced via an
 * AtomicReference. This ensures that readers always get either the new version of these objects or the old version,
 * but not some inconsistent versions of these.
 *
 * It should be noted that this class assumes that there is one writer and multiple readers of the dictionary. It is
 * NOT safe for a multiple writer scenario.
 *
 * TODO
 * - It may be useful to implement a way to stop adding new items when the the number of buffers reaches a certain
 *   threshold. In this case, we could close the realtime segment, and start a new one with bigger buffers.
 */
public abstract class BaseOffHeapMutableDictionary extends MutableDictionary {
  // Percent of cardinality that we expect to hold in the overflow hash table.
  private static final int OVERFLOW_THRESHOLD_PERCENT = 1;
  // List of primes from http://compoasso.free.fr/primelistweb/page/prime/liste_online_en.php
  private static int[] PRIME_NUMBERS = new int[] {20011, 40009, 60013, 80021, 100003, 125113, 150011, 175003, 200003,
      225023, 250007, 275003, 300007,350003, 400009, 450001, 500009, 600011,700001, 800011, 900001, 1000003};

  // expansionMultiple setting as we add new buffers. A setting of 1 sets the new buffer to be
  // the same size as the last one added. Setting of 2 allocates a buffer twice as big as the
  // previous one. It is assumed that these values are powers of 2. It is a good idea to restrict
  // these to 1 or 2, but not higher values. This array can be arbitrarily long. If the number of
  // buffers exceeds the number of elements in the array, we use the last value.
  private static final int[] EXPANSION_MULTIPLES = new int[] {1, 1, 2, 2, 2};

  // Number of columns in each row of an IntBuffer.
  private static final int NUM_COLUMNS = 3;

  // Whether to start with 0 off-heap storage. Items are added to the overflow map first, until it reaches
  // a threshold, and then the off-heap structures are allocated.
  private static final boolean HEAP_FIRST = true;

  private final int _maxItemsInOverflowHash;

  // Number of entries in the dictionary. Max dictId is _numEntries-1.
  private int _numEntries;

  // We keep a list of PinotDataBuffer items from which we get the IntBuffer items, so
  // that we can call close() on these.
  private List<PinotDataBuffer> _pinotDataBuffers = new ArrayList<>();
  private final int _sizeOfFirstBuf;

  /**
   * A class to hold all the objects needed for the reverse mapping.
   */
  private static class ValueToDictId {
    // Each iBuf layout is as described in comments above.
    private final List<IntBuffer> _iBufList;
    // The map should be a concurrent one.
    private final Map<Object, Integer> _overflowMap;

    private ValueToDictId(List<IntBuffer> iBufList, Map<Object, Integer> overflowMap) {
      _iBufList = iBufList;
      _overflowMap = overflowMap;
    }

    private List<IntBuffer> getIBufList() {
      return _iBufList;
    }
    private Map<Object, Integer> getOverflowMap() {
      return _overflowMap;
    }
  }

  private AtomicReference<ValueToDictId> _valueToDict = new AtomicReference<>();

  protected BaseOffHeapMutableDictionary(int estimatedCardinality, int maxOverflowHashSize) {
    final int initialRowCount = nearestPrime(estimatedCardinality);
    _numEntries = 0;
    _maxItemsInOverflowHash = maxOverflowHashSize;
    ValueToDictId valueToDictId = new ValueToDictId(new ArrayList<IntBuffer>(0), new ConcurrentHashMap<Object, Integer>(0));
    _valueToDict.set(valueToDictId);
    _sizeOfFirstBuf = initialRowCount * NUM_COLUMNS;
    if (!HEAP_FIRST || (maxOverflowHashSize == 0)) {
      expand(_sizeOfFirstBuf);
    }
  }

  @Override
  public int indexOf(Object rawValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(int dictionaryId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongValue(int dictionaryId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntValue(int dictionaryId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int length() {
    return _numEntries;
  }

  @Override
  public boolean isEmpty() {
    return _numEntries == 0;
  }

  @Override
  public void close() throws IOException {
    doClose();

    ValueToDictId valueToDictId = _valueToDict.get();
    _valueToDict.set(null);
    Map<Object, Integer> overflowMap = valueToDictId.getOverflowMap();
    overflowMap.clear();
    List<IntBuffer> iBufs = valueToDictId.getIBufList();
    iBufs.clear();

    for (PinotDataBuffer pinotDataBuffer : _pinotDataBuffers) {
      pinotDataBuffer.close();
    }
    _pinotDataBuffers.clear();
  }

  private int nearestPrime(int size) {
    for (int i = 0; i < PRIME_NUMBERS.length; i++) {
      if (PRIME_NUMBERS[i] >= size) {
        return PRIME_NUMBERS[i];
      }
    }
    return PRIME_NUMBERS[PRIME_NUMBERS.length-1];
  }

  private IntBuffer expand(final int newSize) {
    // newSize must be a multiple of NUM_COLUMNS
    final int bbSize = newSize * V1Constants.Numbers.INTEGER_SIZE;
    final int modulo = newSize / NUM_COLUMNS;

    ValueToDictId valueToDictId = _valueToDict.get();
    List<IntBuffer> oldList = valueToDictId.getIBufList();
    List<IntBuffer> newList = new ArrayList<>(oldList.size() + 1);
    for (IntBuffer iBuf : oldList) {
      newList.add(iBuf);
    }
    PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(bbSize);
    _pinotDataBuffers.add(buffer);
    buffer.order(ByteOrder.nativeOrder());
    IntBuffer iBuf = buffer.toDirectByteBuffer(0L, bbSize).asIntBuffer();
    for (int i = 0; i < iBuf.capacity(); i++) {
      iBuf.put(i, NULL_VALUE_INDEX);
    }
    newList.add(iBuf);
    Map<Object, Integer> newOverflowMap = new ConcurrentHashMap<>(_maxItemsInOverflowHash);
    if (_maxItemsInOverflowHash > 0) {
      Map<Object, Integer> oldOverflowMap = valueToDictId.getOverflowMap();
      for (Map.Entry<Object, Integer> entry : oldOverflowMap.entrySet()) {
        final int hashVal = Math.abs(entry.getKey().hashCode());
        final int offsetInBuf = (hashVal % modulo) * NUM_COLUMNS;
        boolean done = false;
        for (int i = offsetInBuf; i < offsetInBuf + NUM_COLUMNS; i++) {
          if (iBuf.get(i) == NULL_VALUE_INDEX) {
            iBuf.put(i, entry.getValue());
            done = true;
            break;
          }
        }
        if (!done) {
          newOverflowMap.put(entry.getKey(), entry.getValue());
        }
      }
    }
    ValueToDictId newValueToDictId = new ValueToDictId(newList, newOverflowMap);
    _valueToDict.set(newValueToDictId);

    // We should not clear oldOverflowMap or oldList here, as readers may still be accessing those.
    // We let GC take care of those elements.
    return iBuf;
  }

  private IntBuffer expand() {
    ValueToDictId valueToDictId = _valueToDict.get();
    List<IntBuffer> iBufList = valueToDictId.getIBufList();
    final int numBuffers = iBufList.size();
    if (numBuffers == 0) {
      return expand(_sizeOfFirstBuf);
    }
    final int lastCapacity = iBufList.get(numBuffers-1).capacity();
    int expansionMultiple = EXPANSION_MULTIPLES[EXPANSION_MULTIPLES.length-1];
    if (numBuffers < EXPANSION_MULTIPLES.length) {
      expansionMultiple = EXPANSION_MULTIPLES[numBuffers];
    }
    int newSize = validateSize(lastCapacity, expansionMultiple);
    return expand(newSize);
  }

  private int validateSize(int lastCapacity, int expansionMultiple) {
    long newSize = (long) lastCapacity * expansionMultiple;
    while (newSize >= Integer.MAX_VALUE) {
      newSize = newSize/2;
    }
    return nearestPrime((int) newSize);
  }

  protected int nearestPowerOf2(int num) {
    if ((num & (num -1)) == 0) {
      return num;
    }
    int power = Integer.SIZE - Integer.numberOfLeadingZeros(num);
    Preconditions.checkState(power < Integer.SIZE-1);
    return 1 << power;
  }

  protected int getDictId(@Nonnull Object rawValue) {
    final int hashVal = Math.abs(rawValue.hashCode());
    final ValueToDictId valueToDictId = _valueToDict.get();
    final List<IntBuffer> iBufList = valueToDictId.getIBufList();
    for (IntBuffer iBuf : iBufList) {
      final int modulo = iBuf.capacity()/NUM_COLUMNS;
      final int offsetInBuf = (hashVal % modulo)  * NUM_COLUMNS;
      for (int i = offsetInBuf; i < offsetInBuf + NUM_COLUMNS; i++) {
        int dictId = iBuf.get(i);
        if (dictId != NULL_VALUE_INDEX) {
          if (rawValue.equals(get(dictId))) {
            return dictId;
          }
        }
      }
    }
    if (_maxItemsInOverflowHash == 0) {
      return NULL_VALUE_INDEX;
    }
    Integer dictId = valueToDictId.getOverflowMap().get(rawValue);
    if (dictId == null) {
      return NULL_VALUE_INDEX;
    }
    return dictId;
  }

  protected void indexValue(@Nonnull Object value) {
    final int hashVal = Math.abs(value.hashCode());
    ValueToDictId valueToDictId = _valueToDict.get();
    final List<IntBuffer> iBufList = valueToDictId.getIBufList();

    for (IntBuffer iBuf : iBufList) {
      final int modulo = iBuf.capacity()/NUM_COLUMNS;
      final int offsetInBuf = (hashVal % modulo) * NUM_COLUMNS;
      for (int i = offsetInBuf; i < offsetInBuf + NUM_COLUMNS; i++) {
        final int dictId = iBuf.get(i);
        if (dictId == NULL_VALUE_INDEX) {
          setRawValueAt(_numEntries, value);
          iBuf.put(i, _numEntries++);
          return;
        }
        if (value.equals(get(dictId))) {
          return;
        }
      }
    }
    // We know that we had a hash collision beyond the number of columns in the buffer.
    Map<Object, Integer> overflowMap = valueToDictId.getOverflowMap();
    if (_maxItemsInOverflowHash > 0) {
      Integer dictId = overflowMap.get(value);
      if (dictId != null) {
        return;
      }
    }

    setRawValueAt(_numEntries, value);

    if (_maxItemsInOverflowHash > 0) {
      if (overflowMap.size() < _maxItemsInOverflowHash) {
        overflowMap.put(value, _numEntries++);
        return;
      }
    }
    // Need a new buffer
    IntBuffer buf = expand();
    final int modulo = buf.capacity()/NUM_COLUMNS;
    final int offsetInBuf = (hashVal % modulo) * NUM_COLUMNS;
    boolean done = false;
    for (int i = offsetInBuf; i < offsetInBuf + NUM_COLUMNS; i++) {
      if (buf.get(i) == NULL_VALUE_INDEX) {
        buf.put(i, _numEntries++);
        done = true;
        break;
      }
    }
    if (_maxItemsInOverflowHash == 0) {
      if (!done) {
        throw new RuntimeException("Impossible");
      }
    }
    if (!done) {
      valueToDictId = _valueToDict.get();
      overflowMap = valueToDictId.getOverflowMap();
      overflowMap.put(value, _numEntries++);
    }
  }

  public long getTotalOffHeapMemUsed() {
    ValueToDictId valueToDictId = _valueToDict.get();
    long size = 0;
    for (IntBuffer iBuf : valueToDictId._iBufList) {
      size = size + iBuf.capacity() * V1Constants.Numbers.INTEGER_SIZE;
    }
    return size;
  }

  public int getNumberOfHeapBuffersUsed() {
    ValueToDictId valueToDictId = _valueToDict.get();
    return valueToDictId._iBufList.size();
  }

  public int getNumberOfOveflowValues() {
    ValueToDictId valueToDictId = _valueToDict.get();
    return valueToDictId._overflowMap.size();
  }

  public int[] getRowFillCount() {
    ValueToDictId valueToDictId = _valueToDict.get();
    int rowsWith1Col[] = new int[NUM_COLUMNS];

    for (int i = 0; i < rowsWith1Col.length; i++) {
      rowsWith1Col[i] = 0;
    }

    for (IntBuffer iBuf : valueToDictId._iBufList) {
      final int nRows = iBuf.capacity()/NUM_COLUMNS;
      for (int row = 0; row < nRows; row++) {
        for (int col = 0; col < NUM_COLUMNS; col++) {
          if (iBuf.get(row * NUM_COLUMNS + col) == NULL_VALUE_INDEX) {
            rowsWith1Col[col]++;
            break;
          }
        }
      }
    }
    return rowsWith1Col;
  }


  public abstract void doClose() throws IOException;

  protected abstract void setRawValueAt(int dictId, Object rawValue);

}
