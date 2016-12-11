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

import alluxio.exception.BlockDoesNotExistException;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Automatically choose the best evictor for the current workloads.
 */
public class EvictorManager implements BlockStoreEventListener {
  /**
   * Thread to check when to switch evictors.
   */
  class AccessCheck implements Runnable {
    /**
     * Constructor for AccessCheck.
     */
    public AccessCheck() {}

    @Override
    public void run() {
      int period = 0;
      int lruPreferPeriods = 0;
      int lirsPreferPeriods = 0;
      double lirsHighHitRate = 0;
      double lirsLowHitRate = 0;
      while (true) {
        try {
          Thread.sleep(3000);
        } catch (InterruptedException ie) {
          ie.printStackTrace();
        }
        if (mHighReuseDistanceBlocks + mLowReuseDistanceBlocks < mLeastAccessBeforeCheck) {
          continue;
        }
        double lruHitBlocks = mLowReuseDistanceHitBlocks;
        double lirsHitBlocks = mHighReuseDistanceBlocks * mLIRSHighRDHitRate
            + mLowReuseDistanceBlocks * mLIRSLowRDHitRate;
        if (Math.abs(lruHitBlocks - lirsHitBlocks) > 1 && lruHitBlocks > lirsHitBlocks) {
          lruPreferPeriods++;
        } else if (Math.abs(lruHitBlocks - lirsHitBlocks) > 1 && lirsHitBlocks > lruHitBlocks) {
          lirsPreferPeriods++;
        }
        lirsHighHitRate += 1.0 * mHighReuseDistanceHitBlocks / mHighReuseDistanceBlocks;
        lirsLowHitRate += 1.0 * mLowReuseDistanceHitBlocks / mLowReuseDistanceBlocks;
        mHighReuseDistanceBlocks = mLowReuseDistanceBlocks = 0;
        mHighReuseDistanceHitBlocks = mLowReuseDistanceHitBlocks = 0;
        period++;
        if (period == mTotalPeriods) {
          if (mEvictor == mCandidateEvictors.get(1)) {
            lirsHighHitRate /= mTotalPeriods;
            lirsLowHitRate /= mTotalPeriods;
            mLIRSHighRDHitRate = mLIRSHighRDHitRate * (1 - mHighRDFeedBackRate)
                + mHighRDFeedBackRate * lirsHighHitRate;
            mLIRSLowRDHitRate =
                mLIRSLowRDHitRate * (1 - mLowRDFeedBackRate) + mLowRDFeedBackRate * lirsLowHitRate;
          }
          if (lruPreferPeriods <= 1 && lirsPreferPeriods > 1) {
            mEvictor = mCandidateEvictors.get(1);
          } else if (lruPreferPeriods > 1 && lirsPreferPeriods <= 1) {
            mEvictor = mCandidateEvictors.get(0);
          } else {
            mEvictor = mCandidateEvictors.get(2);
          }
          period = 0;
          lruPreferPeriods = 0;
          lirsPreferPeriods = 0;
        }
      }
    }
  }

  private static final int LINKED_HASH_MAP_INIT_CAPACITY = 200;
  private static final float LINKED_HASH_MAP_INIT_LOAD_FACTOR = 0.75f;
  private static final boolean LINKED_HASH_MAP_ACCESS_ORDERED = true;

  private final BlockMetadataManagerView mInitManagerView;
  private final Allocator mAllocator;
  private Evictor mEvictor;

  private int mTotalPeriods = 3;
  private int mHighReuseDistanceBlocks = 0;
  private int mLowReuseDistanceBlocks = 0;
  private int mHighReuseDistanceHitBlocks = 0;
  private int mLowReuseDistanceHitBlocks = 0;
  private double mLIRSHighRDHitRate = 0.5;
  private double mLIRSLowRDHitRate = 0.8;
  private double mHighRDFeedBackRate = 0.85;
  private double mLowRDFeedBackRate = 0.85;
  private int mLeastAccessBeforeCheck = 10;

  private final ExecutorService mEvictorCheck =
      Executors.newFixedThreadPool(1, ThreadFactoryUtils.build("Check Evictor", true));
  private Map<Long, BlockStoreLocation> mBlocksOrder = Collections
      .synchronizedMap(new LinkedHashMap<Long, BlockStoreLocation>(LINKED_HASH_MAP_INIT_CAPACITY,
          LINKED_HASH_MAP_INIT_LOAD_FACTOR, LINKED_HASH_MAP_ACCESS_ORDERED));
  private List<Evictor> mCandidateEvictors = new ArrayList<>();

  /**
   * Create an instance of EvictorManager.
   *
   * @param initManagerView view of metadata
   * @param allocator instance of Allocator
   */
  public EvictorManager(BlockMetadataManagerView initManagerView, Allocator allocator) {
    mInitManagerView = initManagerView;
    mAllocator = allocator;
    //mEvictor = Evictor.Factory.create(initManagerView, allocator);
    mCandidateEvictors.add(new LRUEvictor(mInitManagerView, mAllocator));
    // mCandidateEvictors.add(new LRFUEvictor(mInitManagerView, mAllocator));
    mCandidateEvictors.add(new LIRSEvictor(mInitManagerView, mAllocator));
    mCandidateEvictors.add(new ARCEvictor(mInitManagerView, mAllocator));
    mEvictor = mCandidateEvictors.get(0);
    initBlockMetadata();
    mEvictorCheck.submit(new AccessCheck());
  }

  /**
   * Initialize the metadata of current worker.
   */
  private void initBlockMetadata() {
    for (StorageTierView tierView : mInitManagerView.getTierViews()) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        for (BlockMeta blockMeta : dirView.getEvictableBlocks()) {
          mBlocksOrder.put(blockMeta.getBlockId(), dirView.toBlockStoreLocation());
        }
      }
    }
  }

  /**
   * Frees space in the given block store location and with the given view. After eviction, at least
   * one {@link alluxio.worker.block.meta.StorageDir} in the location has the specific amount of
   * free space after eviction. The location can be a specific
   * {@link alluxio.worker.block.meta.StorageDir}, or {@link BlockStoreLocation#anyTier()} or
   * {@link BlockStoreLocation#anyDirInTier(String)}. The view is generated and passed by the
   * calling {@link alluxio.worker.block.BlockStore}.
   * <p>
   * This method returns null if {@link Evictor} fails to propose a feasible plan to meet the
   * requirement, or an eviction plan with toMove and toEvict fields to indicate how to free space.
   * If both toMove and toEvict of the plan are empty, it indicates that {@link Evictor} has no
   * actions to take and the requirement is already met.
   *
   * Throws an {@link IllegalArgumentException} if the given block location is invalid.
   *
   * @param bytesToBeAvailable the amount of free space in bytes to be ensured after eviction
   * @param location the location in block store
   * @param view generated and passed by block store
   * @return an {@link EvictionPlan} (possibly with empty fields) to get the free space, or null if
   *         no plan is feasible
   */
  public EvictionPlan freeSpaceWithView(long bytesToBeAvailable, BlockStoreLocation location,
      BlockMetadataManagerView view) {
    return mEvictor.freeSpaceWithView(bytesToBeAvailable, location, view);
  }

  /**
   * Invoked when a block is accessed.
   *
   * @param sessionId the id of the session to access this block
   * @param blockId the id of the block to access
   */
  public void onAccessBlock(long sessionId, long blockId) {
    try {
      BlockStoreLocation newLocation = mInitManagerView.getBlockMeta(blockId).getBlockLocation();
      // It should be optimized to scan from the recent block.
      boolean last = true;
      long distBytes = 0;
      BlockStoreLocation oldLocation = null;
      for (Iterator<Map.Entry<Long, BlockStoreLocation>> it = mBlocksOrder.entrySet().iterator(); it
          .hasNext();) {
        Map.Entry<Long, BlockStoreLocation> entry = it.next();
        if (last && entry.getKey() != blockId) {
          continue;
        }
        if (entry.getKey() == blockId) {
          oldLocation = entry.getValue();
          last = false;
        }
        if (entry.getValue().belongsTo(oldLocation)) {
          distBytes += mInitManagerView.getBlockMeta(entry.getKey()).getBlockSize();
        }
      }
      StorageDirView dirView =
          mInitManagerView.getTierView(oldLocation.tierAlias()).getDirView(oldLocation.dir());
      long capacity = dirView.getAvailableBytes() + dirView.getEvitableBytes();
      if (distBytes <= capacity) {
        mLowReuseDistanceBlocks++;
        if (oldLocation.equals(newLocation)) {
          mLowReuseDistanceHitBlocks++;
        }
      } else {
        mHighReuseDistanceBlocks++;
        if (oldLocation.equals(newLocation)) {
          mHighReuseDistanceHitBlocks++;
        }
      }
    } catch (BlockDoesNotExistException be) {
      be.printStackTrace();
    }
    for (Evictor evictor : mCandidateEvictors) {
      if (evictor instanceof BlockStoreEventListener) {
        ((BlockStoreEventListener) evictor).onAccessBlock(sessionId, blockId);
      }
    }
  }

  /**
   * Invoked when a block is aborted.
   *
   * @param sessionId the id of the session to abort on this block
   * @param blockId the id of the block where the mutation to abort
   */
  public void onAbortBlock(long sessionId, long blockId) {
    for (Evictor evictor : mCandidateEvictors) {
      if (evictor instanceof BlockStoreEventListener) {
        ((BlockStoreEventListener) evictor).onAbortBlock(sessionId, blockId);
      }
    }
  }

  /**
   * Invoked when a block is committed.
   *
   * @param sessionId the id of the session to commit to this block
   * @param blockId the id of the block to commit
   * @param location the location of the block to be committed
   */
  public void onCommitBlock(long sessionId, long blockId, BlockStoreLocation location) {
    for (Evictor evictor : mCandidateEvictors) {
      if (evictor instanceof BlockStoreEventListener) {
        ((BlockStoreEventListener) evictor).onCommitBlock(sessionId, blockId, location);
      }
    }
  }

  /**
   * Invoked when a block is moved by a client.
   *
   * @param sessionId the id of the session to move this block
   * @param blockId the id of the block to be moved
   * @param oldLocation the source location of the block to be moved
   * @param newLocation the destination location where the block is to be moved to
   */
  public void onMoveBlockByClient(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    for (Evictor evictor : mCandidateEvictors) {
      if (evictor instanceof BlockStoreEventListener) {
        ((BlockStoreEventListener) evictor).onMoveBlockByClient(sessionId, blockId, oldLocation,
            newLocation);
      }
    }
  }

  /**
   * Invoked when a block is moved by a worker.
   *
   * @param sessionId the id of the session to move this block
   * @param blockId the id of the block to be moved
   * @param oldLocation the source location of the block to be moved
   * @param newLocation the destination location where the block is to be moved to
   */
  public void onMoveBlockByWorker(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    for (Evictor evictor : mCandidateEvictors) {
      if (evictor instanceof BlockStoreEventListener) {
        ((BlockStoreEventListener) evictor).onMoveBlockByWorker(sessionId, blockId, oldLocation,
            newLocation);
      }
    }
  }

  /**
   * Invoked when a block is removed by a client.
   *
   * @param sessionId the id of the session to remove this block
   * @param blockId the id of the block to be removed
   */
  public void onRemoveBlockByClient(long sessionId, long blockId) {
    for (Evictor evictor : mCandidateEvictors) {
      if (evictor instanceof BlockStoreEventListener) {
        ((BlockStoreEventListener) evictor).onRemoveBlockByClient(sessionId, blockId);
      }
    }
  }

  /**
   * Invoked when a block is removed by a worker.
   *
   * @param sessionId the id of the session to remove this block
   * @param blockId the id of the block to be removed
   */
  public void onRemoveBlockByWorker(long sessionId, long blockId) {
    for (Evictor evictor : mCandidateEvictors) {
      if (evictor instanceof BlockStoreEventListener) {
        ((BlockStoreEventListener) evictor).onRemoveBlockByWorker(sessionId, blockId);
      }
    }
  }
}
