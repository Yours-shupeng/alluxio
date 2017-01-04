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

package alluxio.worker.block;

import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.resource.LockResource;
import alluxio.worker.block.evictor.EvictorType;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.Set;

/**
 * Block store only contains metadata operations.
 */
public class ShadowBlockStore extends TieredBlockStore {

  /**
   * Create an instance of ShadowBlockStore.
   *
   * @param metaManager block metadata manager
   * @param pinnedInodes inodes pinned in memory
   * @param evictorType type of evictor
   */
  public ShadowBlockStore(BlockMetadataManager metaManager, Set<Long> pinnedInodes,
      EvictorType evictorType) {
    super(metaManager, pinnedInodes, evictorType, true);
  }

  @Override
  protected void abortBlockInternal(long sessionId, long blockId) throws BlockDoesNotExistException,
      BlockAlreadyExistsException, InvalidWorkerStateException, IOException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
    try {
      TempBlockMeta tempBlockMeta;
      try (LockResource r = new LockResource(mMetadataReadLock)) {
        checkTempBlockOwnedBySession(sessionId, blockId);
        tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      }

      try (LockResource r = new LockResource(mMetadataWriteLock)) {
        mMetaManager.abortTempBlockMeta(tempBlockMeta);
      } catch (BlockDoesNotExistException e) {
        throw Throwables.propagate(e); // We shall never reach here
      }
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  @Override
  protected BlockStoreLocation commitBlockInternal(long sessionId, long blockId)
      throws BlockAlreadyExistsException, InvalidWorkerStateException, BlockDoesNotExistException,
      IOException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
    try {
      // When committing TempBlockMeta, the final BlockMeta calculates the block size according to
      // the actual file size of this TempBlockMeta. Therefore, commitTempBlockMeta must happen
      // after moving actual block file to its committed path.
      BlockStoreLocation loc;
      TempBlockMeta tempBlockMeta;
      try (LockResource r = new LockResource(mMetadataReadLock)) {
        checkTempBlockOwnedBySession(sessionId, blockId);
        tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
        loc = tempBlockMeta.getBlockLocation();
        long originWriteBytes = mAliasWriteBytes.get(loc.tierAlias());
        mAliasWriteBytes.put(loc.tierAlias(), originWriteBytes + tempBlockMeta.getBlockSize());
      }
      try (LockResource r = new LockResource(mMetadataWriteLock)) {
        mMetaManager.commitTempBlockMeta(tempBlockMeta);
      } catch (BlockAlreadyExistsException | BlockDoesNotExistException
          | WorkerOutOfSpaceException e) {
        throw Throwables.propagate(e); // we shall never reach here
      }
      return loc;
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  @Override
  protected MoveBlockResult moveBlockInternal(long sessionId, long blockId,
      BlockStoreLocation oldLocation, BlockStoreLocation newLocation)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      IOException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
    try {
      long blockSize;
      String srcFilePath;
      String dstFilePath;
      BlockMeta srcBlockMeta;
      BlockStoreLocation srcLocation;
      BlockStoreLocation dstLocation;

      try (LockResource r = new LockResource(mMetadataReadLock)) {
        if (mMetaManager.hasTempBlockMeta(blockId)) {
          throw new InvalidWorkerStateException(ExceptionMessage.MOVE_UNCOMMITTED_BLOCK, blockId);
        }
        srcBlockMeta = mMetaManager.getBlockMeta(blockId);
        srcLocation = srcBlockMeta.getBlockLocation();
        srcFilePath = srcBlockMeta.getPath();
        blockSize = srcBlockMeta.getBlockSize();
      }

      if (!srcLocation.belongsTo(oldLocation)) {
        throw new BlockDoesNotExistException(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION, blockId,
            oldLocation);
      }
      TempBlockMeta dstTempBlock =
          createBlockMetaInternal(sessionId, blockId, newLocation, blockSize, false);
      if (dstTempBlock == null) {
        return new MoveBlockResult(false, blockSize, null, null);
      }

      // When `newLocation` is some specific location, the `newLocation` and the `dstLocation` are
      // just the same; while for `newLocation` with a wildcard significance, the `dstLocation`
      // is a specific one with specific tier and dir which belongs to newLocation.
      dstLocation = dstTempBlock.getBlockLocation();

      // When the dstLocation belongs to srcLocation, simply abort the tempBlockMeta just created
      // internally from the newLocation and return success with specific block location.
      if (dstLocation.belongsTo(srcLocation)) {
        mMetaManager.abortTempBlockMeta(dstTempBlock);
        return new MoveBlockResult(true, blockSize, srcLocation, dstLocation);
      }
      try (LockResource r = new LockResource(mMetadataWriteLock)) {
        // If this metadata update fails, we panic for now.
        // TODO(bin): Implement rollback scheme to recover from IO failures.
        mMetaManager.moveBlockMeta(srcBlockMeta, dstTempBlock);
        long originWriteBytes = mAliasWriteBytes.get(dstLocation.tierAlias());
        long originReadBytes = mAliasReadBytes.get(srcLocation.tierAlias());
        mAliasWriteBytes.put(dstLocation.tierAlias(), originWriteBytes + blockSize);
        mAliasReadBytes.put(srcLocation.tierAlias(), originReadBytes + blockSize);
      } catch (BlockAlreadyExistsException | BlockDoesNotExistException
          | WorkerOutOfSpaceException e) {
        // WorkerOutOfSpaceException is only possible if session id gets cleaned between
        // createBlockMetaInternal and moveBlockMeta.
        throw Throwables.propagate(e); // we shall never reach here
      }

      return new MoveBlockResult(true, blockSize, srcLocation, dstLocation);
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  @Override
  protected void removeBlockInternal(long sessionId, long blockId, BlockStoreLocation location)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
    try {
      BlockMeta blockMeta;
      try (LockResource r = new LockResource(mMetadataReadLock)) {
        if (mMetaManager.hasTempBlockMeta(blockId)) {
          throw new InvalidWorkerStateException(ExceptionMessage.REMOVE_UNCOMMITTED_BLOCK, blockId);
        }
        blockMeta = mMetaManager.getBlockMeta(blockId);
      }

      if (!blockMeta.getBlockLocation().belongsTo(location)) {
        throw new BlockDoesNotExistException(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION, blockId,
            location);
      }
      try (LockResource r = new LockResource(mMetadataWriteLock)) {
        mMetaManager.removeBlockMeta(blockMeta);
      } catch (BlockDoesNotExistException e) {
        throw Throwables.propagate(e); // we shall never reach here
      }
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }
}

