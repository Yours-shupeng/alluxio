/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.evictor;

import java.io.IOException;

/**
 * Common type of evictors.
 */
public enum EvictorType {
  /**
   * Corresponding to LRUEvictor.
   */
  LRU(1),

  /**
   * Corresponding to LRFUEvictor.
   */
  LRFU(2),

  /**
   * Corresponding to LIRSEvictor.
   */
  LIRS(3),

  /**
   * Corresponding to ARCEvictor.
   */
  ARC(4);

  private final int mValue;

  EvictorType(int value) {
    mValue = value;
  }

  /**
   * @return the Evictor type value
   */
  public int getValue() {
    return mValue;
  }

  /**
   * Get EvictorType of value.
   *
   * @param value int value
   * @return EvictorType corresponding to the value
   * @throws IOException if no EvictorType has the value
   */
  public static EvictorType getEvictorType(int value) throws IOException {
    switch (value) {
      case 1:
        return EvictorType.LRU;
      case 2:
        return EvictorType.LRFU;
      case 3:
        return EvictorType.LIRS;
      case 4:
        return EvictorType.ARC;
      default:
        return null;
    }
  }
}
