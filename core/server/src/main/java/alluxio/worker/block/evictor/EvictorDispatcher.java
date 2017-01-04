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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.collections.Pair;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.StorageDirView;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Adaptively choose the best Evictor according to the current access behavior.
 * Wrong!!! Each evictor has independent behaviours will cause different memory state.
 */
public class EvictorDispatcher extends Thread implements BlockStoreEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private Map<EvictorType, Evictor> mEvictors = new HashMap<>();
  private Map<EvictorType, Long> mLost = new HashMap<>();
  private Evictor mCurrentEvictor;
  private EvictorType mCurrentEvictorType;

  private long mMemBytes = 0;
  private long mAccessedBytes = 0;

  private BlockMetadataManagerView mBlockMetadataManagerView;
  private Allocator mAllocator;

  /**
   * Create an instance of EvictorDispatcher.
   *
   * @param blockMetadataManagerView instance of BlockMetadataManagerView
   * @param allocator instance of Allocator
   */
  public EvictorDispatcher(BlockMetadataManagerView blockMetadataManagerView, Allocator allocator) {
    mBlockMetadataManagerView = blockMetadataManagerView;
    mAllocator = allocator;
    initEvictors();
    for (StorageDirView dir : mBlockMetadataManagerView.getTierView("MEM").getDirViews()) {
      mMemBytes += dir.getAvailableBytes() + dir.getEvitableBytes();
    }
  }

  /**
   * Start the thread and check periodically.
   */
  @Override
  public void run() {
    while (true) {
      try {
        Thread.sleep(10000);
        if (!Configuration.getBoolean(PropertyKey.WORKER_EVICTOR_AUTO_ENABLED)) {
          continue;
        }
      } catch (InterruptedException e) {
        LOG.error("Failed to sleep seconds because {}", e);
      }
      if (mAccessedBytes < 3 * mMemBytes) {
        continue;
      }
      long minLost = Long.MAX_VALUE;
      Evictor candidateEvictor = mCurrentEvictor;
      EvictorType candidateEvictorType = mCurrentEvictorType;
      for (Iterator<Map.Entry<EvictorType, Evictor>> it = mEvictors.entrySet().iterator(); it
          .hasNext();) {
        Map.Entry<EvictorType, Evictor> entry = it.next();
        Evictor evictor = entry.getValue();
        EvictorType evictorType = entry.getKey();
        System.out.println(evictorType + ": lost: " + mLost.get(evictorType));
        if (mLost.get(evictorType) < minLost) {
          minLost = mLost.get(evictorType);
          candidateEvictor = evictor;
          candidateEvictorType = evictorType;
        } else if (mLost.get(evictorType) == minLost && evictorType == EvictorType.LIRS) {
          candidateEvictor = evictor;
          candidateEvictorType = evictorType;
        }
      }
      for (EvictorType evictorType : mEvictors.keySet()) {
        if (evictorType != mCurrentEvictorType || candidateEvictor != mCurrentEvictor) {
          switch (evictorType) {
            case LRU:
              mEvictors.put(evictorType, new LRUEvictor(mBlockMetadataManagerView, mAllocator));
              break;
            case LRFU:
              mEvictors.put(evictorType, new LRFUEvictor(mBlockMetadataManagerView, mAllocator));
              break;
            case LIRS:
              mEvictors.put(evictorType, new LIRSEvictor(mBlockMetadataManagerView, mAllocator));
              break;
            case ARC:
              mEvictors.put(evictorType, new ARCEvictor(mBlockMetadataManagerView, mAllocator));
              break;
            default:
              LOG.error("Failed to switch evictor because the wrong evictor type!");
          }
        }
        mLost.put(evictorType, 0L);
      }
      if (candidateEvictor != mCurrentEvictor) {
        LOG.info("Switch evictor from {} to {}", mCurrentEvictorType, candidateEvictorType);
        mCurrentEvictorType = candidateEvictorType;
        mCurrentEvictor = mEvictors.get(mCurrentEvictorType);
      }
      mAccessedBytes = 0L;
    }
  }

  /**
   * Simulate free space with evictors.
   *
   * @param sessionId id of session
   * @param bytesToBeAvailable bytes to evict
   * @param location the location in block store
   * @param view generated and passed by block store
   * @return eviction plan
   * @throws WorkerOutOfSpaceException if no plan is generated
   * @throws BlockDoesNotExistException if the block does not exist
   */
  public EvictionPlan freeSpaceWithView(long sessionId, long bytesToBeAvailable,
      BlockStoreLocation location, BlockMetadataManagerView view) throws WorkerOutOfSpaceException {
    if (Configuration.getBoolean(PropertyKey.WORKER_EVICTOR_AUTO_ENABLED)) {
      for (Iterator<Map.Entry<EvictorType, Evictor>> it = mEvictors.entrySet().iterator(); it
          .hasNext();) {
        Map.Entry<EvictorType, Evictor> entry = it.next();
        Evictor evictor = entry.getValue();
        EvictorType evictorType = entry.getKey();
        if (evictor == mCurrentEvictor) {
          continue;
        }
        EvictionPlan plan = evictor.freeSpaceWithView(bytesToBeAvailable, location, view);
        // TODO(shupeng) choose whether to copy evictors or not
        if (plan == null) {
          throw new WorkerOutOfSpaceException(ExceptionMessage.NO_EVICTION_PLAN_TO_FREE_SPACE);
        }
        for (Pair<Long, BlockStoreLocation> blockInfo : plan.toEvict()) {
          ((BlockStoreEventListener) evictor).onRemoveBlockByWorker(sessionId,
              blockInfo.getFirst());
        }
        for (BlockTransferInfo blockTransferInfo : plan.toMove()) {
          BlockStoreLocation oldLocation = blockTransferInfo.getSrcLocation();
          BlockStoreLocation newLocation = blockTransferInfo.getDstLocation();
          if (oldLocation == null || newLocation == null) {
            continue;
          }
          try {
            mLost.put(evictorType, mLost.get(evictorType) + mBlockMetadataManagerView
                .getBlockMeta(blockTransferInfo.getBlockId()).getBlockSize());
          } catch (BlockDoesNotExistException e) {
            LOG.error("Failed to update lost caused by eviction because {}", e);
          }
          ((BlockStoreEventListener) evictor).onMoveBlockByWorker(sessionId,
              blockTransferInfo.getBlockId(), blockTransferInfo.getSrcLocation(),
              blockTransferInfo.getDstLocation());
        }
      }
    }
    return mCurrentEvictor.freeSpaceWithView(bytesToBeAvailable, location, view);
  }

  private void initEvictors() {
    mEvictors.put(EvictorType.LRU, new LRUEvictor(mBlockMetadataManagerView, mAllocator));
    mEvictors.put(EvictorType.LRFU, new LRFUEvictor(mBlockMetadataManagerView, mAllocator));
    mEvictors.put(EvictorType.LIRS, new LIRSEvictor(mBlockMetadataManagerView, mAllocator));
    mEvictors.put(EvictorType.ARC, new ARCEvictor(mBlockMetadataManagerView, mAllocator));
    try {
      mCurrentEvictor = CommonUtils.createNewClassInstance(
          Configuration.<Evictor>getClass(PropertyKey.WORKER_EVICTOR_CLASS),
          new Class[] {BlockMetadataManagerView.class, Allocator.class},
          new Object[] {mBlockMetadataManagerView, mAllocator});
      mCurrentEvictorType =
          EvictorType.getEvictorType(Configuration.getInt(PropertyKey.WORKER_EVICTOR_TYPE));
      mEvictors.put(mCurrentEvictorType, mCurrentEvictor);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    mLost.put(EvictorType.LRU, 0L);
    mLost.put(EvictorType.LRFU, 0L);
    mLost.put(EvictorType.LIRS, 0L);
    mLost.put(EvictorType.ARC, 0L);
  }

  /**
   * Action when accessing a block.
   *
   * @param sessionId the id of the session to access this block
   * @param blockId the id of the block to access
   */
  public void onAccessBlock(long sessionId, long blockId) {
    System.out.println("Access block " + blockId);
    try {
      mAccessedBytes += mBlockMetadataManagerView.getBlockMeta(blockId).getBlockSize();
    } catch (BlockDoesNotExistException e) {
      LOG.error("Block does not exist because {}", e);
    }
    if (Configuration.getBoolean(PropertyKey.WORKER_EVICTOR_AUTO_ENABLED)) {
      for (Evictor evictor : mEvictors.values()) {
        ((BlockStoreEventListener) evictor).onAccessBlock(sessionId, blockId);
      }
    } else {
      ((BlockStoreEventListener) mCurrentEvictor).onAccessBlock(sessionId, blockId);
    }
  }

  /**
   * Action when aborting a block.
   *
   * @param sessionId the id of the session to abort on this block
   * @param blockId the id of the block where the mutation to abort
   */
  public void onAbortBlock(long sessionId, long blockId) {
    if (Configuration.getBoolean(PropertyKey.WORKER_EVICTOR_AUTO_ENABLED)) {
      for (Evictor evictor : mEvictors.values()) {
        ((BlockStoreEventListener) evictor).onAbortBlock(sessionId, blockId);
      }
    } else {
      ((BlockStoreEventListener) mCurrentEvictor).onAbortBlock(sessionId, blockId);
    }
  }

  /**
   * Action when committing a block.
   *
   * @param sessionId the id of the session to commit to this block
   * @param blockId the id of the block to commit
   * @param location the location of the block to be committed
   */
  public void onCommitBlock(long sessionId, long blockId, BlockStoreLocation location) {
    System.out.println("Commit block " + blockId);
    try {
      mAccessedBytes += mBlockMetadataManagerView.getBlockMeta(blockId).getBlockSize();
    } catch (BlockDoesNotExistException e) {
      LOG.error("Block does not exist because {}", e);
    }
    if (Configuration.getBoolean(PropertyKey.WORKER_EVICTOR_AUTO_ENABLED)) {
      for (Evictor evictor : mEvictors.values()) {
        ((BlockStoreEventListener) evictor).onCommitBlock(sessionId, blockId, location);
      }
    } else {
      ((BlockStoreEventListener) mCurrentEvictor).onCommitBlock(sessionId, blockId, location);
    }
  }

  /**
   * Action when moving a block by client.
   *
   * @param sessionId the id of the session to move this block
   * @param blockId the id of the block to be moved
   * @param oldLocation the source location of the block to be moved
   * @param newLocation the destination location where the block is to be moved to
   */
  public void onMoveBlockByClient(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    if (oldLocation == null || newLocation == null) {
      return;
    }
    int oldTierOrd =
        mBlockMetadataManagerView.getTierView(oldLocation.tierAlias()).getTierViewOrdinal();
    int newTierOrd =
        mBlockMetadataManagerView.getTierView(newLocation.tierAlias()).getTierViewOrdinal();
    if (oldTierOrd < newTierOrd) {
      try {
        mLost.put(mCurrentEvictorType, mLost.get(mCurrentEvictorType)
            + mBlockMetadataManagerView.getBlockMeta(blockId).getBlockSize());
      } catch (BlockDoesNotExistException e) {
        LOG.error("The metadata of block {} cannot be found because {}", blockId, e);
      }
    }
    if (Configuration.getBoolean(PropertyKey.WORKER_EVICTOR_AUTO_ENABLED)) {
      for (Evictor evictor : mEvictors.values()) {
        ((BlockStoreEventListener) evictor).onMoveBlockByClient(sessionId, blockId, oldLocation,
            newLocation);
      }
    } else {
      ((BlockStoreEventListener) mCurrentEvictor).onMoveBlockByClient(sessionId, blockId,
          oldLocation, newLocation);
    }
  }

  /**
   * Action when moving a block by worker.
   *
   * @param sessionId the id of the session to move this block
   * @param blockId the id of the block to be moved
   * @param oldLocation the source location of the block to be moved
   * @param newLocation the destination location where the block is to be moved to
   */
  public void onMoveBlockByWorker(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    if (oldLocation == null || newLocation == null) {
      return;
    }
    int oldTierOrd =
        mBlockMetadataManagerView.getTierView(oldLocation.tierAlias()).getTierViewOrdinal();
    int newTierOrd =
        mBlockMetadataManagerView.getTierView(newLocation.tierAlias()).getTierViewOrdinal();
    if (oldTierOrd < newTierOrd) {
      try {
        mLost.put(mCurrentEvictorType, mLost.get(mCurrentEvictorType)
            + mBlockMetadataManagerView.getBlockMeta(blockId).getBlockSize());
      } catch (BlockDoesNotExistException e) {
        LOG.error("The metadata of block {} cannot be found because {}", blockId, e);
      }
    }
    ((BlockStoreEventListener) mCurrentEvictor).onMoveBlockByWorker(sessionId, blockId, oldLocation,
        newLocation);
  }

  /**
   * Action when removing a block by client.
   *
   * @param sessionId the id of the session to remove this block
   * @param blockId the id of the block to be removed
   */
  public void onRemoveBlockByClient(long sessionId, long blockId) {
    if (Configuration.getBoolean(PropertyKey.WORKER_EVICTOR_AUTO_ENABLED)) {
      for (Evictor evictor : mEvictors.values()) {
        ((BlockStoreEventListener) evictor).onRemoveBlockByClient(sessionId, blockId);
      }
    } else {
      ((BlockStoreEventListener) mCurrentEvictor).onRemoveBlockByClient(sessionId, blockId);
    }
  }

  /**
   * Action when removing a block by worker.
   *
   * @param sessionId the id of the session to remove this block
   * @param blockId the id of the block to be removed
   */
  public void onRemoveBlockByWorker(long sessionId, long blockId) {
    ((BlockStoreEventListener) mCurrentEvictor).onRemoveBlockByWorker(sessionId, blockId);
  }
}
