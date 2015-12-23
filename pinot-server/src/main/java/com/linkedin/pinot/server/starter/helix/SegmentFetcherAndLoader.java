/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.server.starter.helix;

import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


public class SegmentFetcherAndLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentFetcherAndLoader.class);

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final DataManager _dataManager;
  private final SegmentMetadataLoader _metadataLoader;
  private final String _instanceId;

  private final int _segmentLoadMaxRetryCount;
  private final long _segmentLoadMinRetryDelayMs; // Min delay (in msecs) between retries

  public SegmentFetcherAndLoader(DataManager dataManager, SegmentMetadataLoader metadataLoader,
      ZkHelixPropertyStore<ZNRecord> propertyStore, Configuration pinotHelixProperties,
      String instanceId) {
    _propertyStore = propertyStore;
    _dataManager = dataManager;
    _metadataLoader = metadataLoader;
    _instanceId = instanceId;
    int maxRetries = Integer.parseInt(CommonConstants.Server.DEFAULT_SEGMENT_LOAD_MAX_RETRY_COUNT);
    try {
      maxRetries = pinotHelixProperties
          .getInt(CommonConstants.Server.CONFIG_OF_SEGMENT_LOAD_MAX_RETRY_COUNT, maxRetries);
    } catch (Exception e) {
      // Keep the default value
    }
    _segmentLoadMaxRetryCount = maxRetries;

    long minRetryDelayMillis =
        Long.parseLong(CommonConstants.Server.DEFAULT_SEGMENT_LOAD_MIN_RETRY_DELAY_MILLIS);
    try {
      minRetryDelayMillis = pinotHelixProperties.getLong(
          CommonConstants.Server.CONFIG_OF_SEGMENT_LOAD_MIN_RETRY_DELAY_MILLIS,
          minRetryDelayMillis);
    } catch (Exception e) {
      // Keep the default value
    }
    _segmentLoadMinRetryDelayMs = minRetryDelayMillis;
  }

  public void addOrReplaceOfflineSegment(String tableName, String segmentId, boolean retryOnFailure) {
    OfflineSegmentZKMetadata offlineSegmentZKMetadata =
        ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, tableName, segmentId);

    LOGGER.info("Adding or replacing segment {} for table {}", segmentId, tableName);
    try {
      SegmentMetadata segmentMetadataForCheck = new SegmentMetadataImpl(offlineSegmentZKMetadata);

      // We lock the segment in order to get its metadata, and then release the lock, so it is possible
      // that the segment is dropped after we get its metadata.
      SegmentMetadata localSegmentMetadata = _dataManager.getSegmentMetadata(tableName, segmentId);

      if (localSegmentMetadata == null) {
        LOGGER.info("Segment {} is not loaded in memory, checking disk", segmentId);
        final String localSegmentDir = getSegmentLocalDirectory(tableName, segmentId);
        if (new File(localSegmentDir).exists()) {
          LOGGER.info("Segment {} found on disk, attempting to load it", segmentId);
          try {
            localSegmentMetadata = _metadataLoader.loadIndexSegmentMetadataFromDir(localSegmentDir);
          } catch (Exception e) {
            LOGGER.error("Failed to load segment metadata from {}. Deleting it.", localSegmentDir, e);
            FileUtils.deleteQuietly(new File(localSegmentDir));
            localSegmentMetadata = null;
          }
          try {
            if (!isNewSegmentMetadata(localSegmentMetadata, segmentMetadataForCheck)) {
              LOGGER.info("Segment metadata same as before, loading {} from disk", segmentId);
              AbstractTableConfig tableConfig = ZKMetadataProvider.getOfflineTableConfig(_propertyStore, tableName);
              _dataManager.addSegment(localSegmentMetadata, tableConfig);
              // TODO Update zk metadata with CRC for this instance
              return;
            }
          } catch (Exception e) {
            LOGGER.error("Failed to load {} from local, will try to reload it from controller!", segmentId, e);
            FileUtils.deleteQuietly(new File(localSegmentDir));
            localSegmentMetadata = null;
          }
        }
      }
      // There is a very unlikely race condition that we may have gotten the metadata of a
      // segment that was not dropped when we checked, but was dropped after the check above.
      // That is possible only if we get two helix transitions (to drop, and to add back) the
      // segment at the same, or very close to each other.If the race condition triggers, and the
      // two segments are same in metadata, then we may end up NOT adding back the segment
      // that is in the process of being dropped.

      // If we get here, then either it is the case that we have the segment loaded in memory (and therefore present
      // in disk) or, we need to load from the server. In the former case, we still need to check if the metadata
      // that we have is different from that in zookeeper.
      if (isNewSegmentMetadata(localSegmentMetadata, segmentMetadataForCheck)) {
        if (localSegmentMetadata == null) {
          LOGGER .info("Loading new segment from controller - " + segmentId);
        } else {
          LOGGER.info("Trying to refresh segment {} with new data.", segmentId);
        }
        int retryCount;
        int maxRetryCount = 1;
        if (retryOnFailure) {
          maxRetryCount = _segmentLoadMaxRetryCount;
        }
        for (retryCount = 0; retryCount < maxRetryCount; ++retryCount) {
          long attemptStartTime = System.currentTimeMillis();
          try {
            AbstractTableConfig tableConfig = ZKMetadataProvider.getOfflineTableConfig(_propertyStore, tableName);
            final String uri = offlineSegmentZKMetadata.getDownloadUrl();
            final String localSegmentDir = downloadSegmentToLocal(uri, tableName, segmentId);
            final SegmentMetadata segmentMetadata =
                _metadataLoader.loadIndexSegmentMetadataFromDir(localSegmentDir);
            _dataManager.addSegment(segmentMetadata, tableConfig);
            // TODO Update zk metadata with CRC for this instance

            // Successfully loaded the segment, break out of the retry loop
            break;
          } catch (Exception e) {
            long attemptDurationMillis = System.currentTimeMillis() - attemptStartTime;
            LOGGER.warn("Caught exception while loading segment " + segmentId + ", attempt "
                + (retryCount + 1) + " of " + maxRetryCount, e);

            // Do we need to wait for the next retry attempt?
            if (retryCount < maxRetryCount-1) {
              // Exponentially back off, wait for (minDuration + attemptDurationMillis) *
              // 1.0..(2^retryCount)+1.0
              double maxRetryDurationMultiplier = Math.pow(2.0, (retryCount + 1));
              double retryDurationMultiplier = Math.random() * maxRetryDurationMultiplier + 1.0;
              long waitTime =
                  (long) ((_segmentLoadMinRetryDelayMs + attemptDurationMillis) * retryDurationMultiplier);

              LOGGER.warn("Waiting for " + TimeUnit.MILLISECONDS.toSeconds(waitTime)
                  + " seconds to retry");
              long waitEndTime = System.currentTimeMillis() + waitTime;
              while (System.currentTimeMillis() < waitEndTime) {
                try {
                  Thread.sleep(Math.max(System.currentTimeMillis() - waitEndTime, 1L));
                } catch (InterruptedException ie) {
                  // Ignore spurious wakeup
                }
              }
            }
          }
        }
        if (_segmentLoadMaxRetryCount <= retryCount) {
          String msg =
              "Failed to download segment " + segmentId + " after " + retryCount + " retries";
          LOGGER.error(msg);
          throw new RuntimeException(msg);
        }
      } else {
        LOGGER.info("Get already loaded segment again, will do nothing.");
      }
    } catch (final Exception e) {
      LOGGER.error("Cannot load segment : " + segmentId + "!\n", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }
  }

  private boolean isNewSegmentMetadata(SegmentMetadata segmentMetadataFromServer,
      SegmentMetadata segmentMetadataForCheck) {
    if (segmentMetadataFromServer == null || segmentMetadataForCheck == null) {
      LOGGER.info("segmentMetadataForCheck = null? {}, segmentMetadataFromServer = null? {}",
          segmentMetadataForCheck == null, segmentMetadataFromServer == null);
      return true;
    }
    LOGGER.info("segmentMetadataForCheck.crc={},segmentMetadataFromServer.crc={}",
        segmentMetadataForCheck.getCrc(), segmentMetadataFromServer.getCrc());
    if ((!segmentMetadataFromServer.getCrc().equalsIgnoreCase("null"))
        && (segmentMetadataFromServer.getCrc().equals(segmentMetadataForCheck.getCrc()))) {
      return false;
    }
    return true;
  }

  private String downloadSegmentToLocal(String uri, String tableName, String segmentId)
      throws Exception {
    File tempSegmentFile = null;
    File tempFile = null;
    if (uri.startsWith("hdfs:")) {
      throw new UnsupportedOperationException("Not implemented yet");
    } else {
      try {
        tempSegmentFile = new File(_dataManager.getSegmentFileDirectory() + "/"
            + tableName + "/temp_" + segmentId + "_" + System.currentTimeMillis());
        if (uri.startsWith("http:") || uri.startsWith("https:")) {
          tempFile = new File(_dataManager.getSegmentFileDirectory(), segmentId + ".tar.gz");
          final long httpGetResponseContentLength = FileUploadUtils.getFile(uri, tempFile);
          LOGGER.info("Downloaded file from " + uri + " to " + tempFile
              + "; Http GET response content length: " + httpGetResponseContentLength
              + ", Length of downloaded file : " + tempFile.length());
          LOGGER.info("Trying to uncompress segment tar file from " + tempFile + " to "
              + tempSegmentFile);
          TarGzCompressionUtils.unTar(tempFile, tempSegmentFile);
          FileUtils.deleteQuietly(tempFile);
        } else {
          TarGzCompressionUtils.unTar(new File(uri), tempSegmentFile);
        }
        final File segmentDir = new File(new File(_dataManager.getSegmentDataDirectory(), tableName), segmentId);
        Thread.sleep(1000);
        if (segmentDir.exists()) {
          LOGGER.info("Deleting the directory and recreating it again- " + segmentDir.getAbsolutePath());
          FileUtils.deleteDirectory(segmentDir);
        }
        LOGGER.info("Move the dir - " + tempSegmentFile.listFiles()[0] + " to "
            + segmentDir.getAbsolutePath() + ". The segment id is - " + segmentId);
        FileUtils.moveDirectory(tempSegmentFile.listFiles()[0], segmentDir);
        FileUtils.deleteDirectory(tempSegmentFile);
        Thread.sleep(1000);
        LOGGER.info("Was able to succesfully rename the dir to match the segmentId - " + segmentId);

        new File(segmentDir, "finishedLoading").createNewFile();
        return segmentDir.getAbsolutePath();
      } catch (Exception e) {
        FileUtils.deleteQuietly(tempSegmentFile);
        FileUtils.deleteQuietly(tempFile);
        LOGGER.error("Caught exception", e);
        Utils.rethrowException(e);
        throw new AssertionError("Should not reach this");
      }
    }
  }

  public String getSegmentLocalDirectory(String tableName, String segmentId) {
    return _dataManager.getSegmentDataDirectory() + "/" + tableName + "/" + segmentId;
  }

}