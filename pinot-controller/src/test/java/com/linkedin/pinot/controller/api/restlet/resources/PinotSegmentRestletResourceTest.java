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

package com.linkedin.pinot.controller.api.restlet.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTest;

import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotSegmentRestletResourceTest extends ControllerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRestletResourceTest.class);
  ControllerRequestURLBuilder urlBuilder = ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL);

  private PinotHelixResourceManager _pinotHelixResourceManager;
  private final static String ZK_SERVER = ZkStarter.DEFAULT_ZK_STR;
  private final static String TABLE_NAME = "testTable";

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    startController();
    _pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();

    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(getHelixClusterName(), ZK_SERVER, 1, true);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(getHelixClusterName(), ZK_SERVER, 1, true);

    Thread.sleep(3000);

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_OFFLINE").size(), 1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_BROKER").size(), 1);
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopController();
    stopZk();
  }

  @Test
  public void testSegmentCrcApi() throws Exception {
    // Adding table
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TABLE_NAME)
            .setNumReplicas(1)
            .build();
    _pinotHelixResourceManager.addTable(tableConfig);
    Thread.sleep(3000);

    // Upload Segments
    List<SegmentMetadata> segmentMetadataList = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      SegmentMetadata metadata = addOneSegment(TABLE_NAME);
      segmentMetadataList.add(metadata);
    }

    // Get crc info from API and check that they are correct.
    ObjectMapper mapper = new ObjectMapper();
    String crcMapStr = sendGetRequest(urlBuilder.forListAllCrcInformationForTable(TABLE_NAME));
    Map<String, String> crcMap = mapper.readValue(crcMapStr, new TypeReference<Map<String, Object>>(){});
    for(SegmentMetadata metadata : segmentMetadataList) {
      String segmentName = metadata.getName();
      String crc = metadata.getCrc();
      Assert.assertEquals(crcMap.get(segmentName), crc);
    }

    // Add more segments
    for (int i = 0; i < 5; ++i) {
      SegmentMetadata metadata = addOneSegment(TABLE_NAME);
      segmentMetadataList.add(metadata);
    }

    // Get crc info from API and check that they are correct.
    crcMapStr = sendGetRequest(urlBuilder.forListAllCrcInformationForTable(TABLE_NAME));
    crcMap = mapper.readValue(crcMapStr, new TypeReference<Map<String, Object>>(){});
    for(SegmentMetadata metadata : segmentMetadataList) {
      String segmentName = metadata.getName();
      String crc = metadata.getCrc();
      Assert.assertEquals(crcMap.get(segmentName), crc);
    }
  }

  private SegmentMetadata addOneSegment(String resourceName) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(resourceName);
    LOGGER.info("Trying to add IndexSegment : " + segmentMetadata.getName());
    _pinotHelixResourceManager.addSegment(segmentMetadata, "downloadUrl");
    return segmentMetadata;
  }
}
