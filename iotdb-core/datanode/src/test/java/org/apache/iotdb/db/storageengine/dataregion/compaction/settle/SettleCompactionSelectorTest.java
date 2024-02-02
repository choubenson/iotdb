package org.apache.iotdb.db.storageengine.dataregion.compaction.settle;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SettleSelectorImpl;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class SettleCompactionSelectorTest extends AbstractCompactionTest {

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    DataNodeTTLCache.getInstance().clearAllTTL();
  }

  @Test
  public void testSelectContinuousFileWithLightSelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, false);

    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // inner task, continuous
    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(false, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());
    Assert.assertEquals(1, (seqTasks.get(0).getPartialDeletedFileGroups().size()));
    Assert.assertEquals(0, seqTasks.get(0).getAllDeletedFiles().size());
    Assert.assertEquals(1, (unseqTasks.get(0).getPartialDeletedFileGroups().size()));
    Assert.assertEquals(0, unseqTasks.get(0).getAllDeletedFiles().size());
    Assert.assertEquals(3, seqTasks.get(0).getPartialDeletedFileGroups().get(0).size());
    Assert.assertEquals(3, unseqTasks.get(0).getPartialDeletedFileGroups().get(0).size());

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(3, tsFileManager.getTsFileList(false).size());

    Assert.assertFalse(tsFileManager.getTsFileList(true).get(0).getModFile().exists());
    Assert.assertFalse(tsFileManager.getTsFileList(false).get(0).getModFile().exists());

    // select second time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());
    Assert.assertEquals(2, (seqTasks.get(0).getPartialDeletedFileGroups().get(0).size()));
    Assert.assertEquals(2, (unseqTasks.get(0).getPartialDeletedFileGroups().get(0).size()));

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());
    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
      Assert.assertFalse(tsFileManager.getTsFileList(false).get(i).getModFile().exists());
    }

    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  @Test
  public void testSelectUnContinuousFileWithLightSelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, false, false);

    // file 0 2 4 6 8 has mods file, whose size is over threshold
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        generateModsFile(5, 10, Collections.singletonList(seqResources.get(i)), 0, Long.MAX_VALUE);
        generateModsFile(
            5, 10, Collections.singletonList(unseqResources.get(i)), 0, Long.MAX_VALUE);
      }
    }
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // settle task, not continuous
    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(false, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    int num = 10;
    for (int i = 0; i < 5; i++) {
      List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
      List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
      Assert.assertEquals(1, seqTasks.size());
      Assert.assertEquals(1, unseqTasks.size());
      Assert.assertEquals(1, (seqTasks.get(0).getPartialDeletedFileGroups().size()));
      Assert.assertEquals(0, (seqTasks.get(0).getAllDeletedFiles().size()));
      Assert.assertEquals(1, seqTasks.get(0).getTotalPartialDeletedFilesNum());
      Assert.assertEquals(1, (unseqTasks.get(0).getPartialDeletedFileGroups().size()));
      Assert.assertEquals(0, (unseqTasks.get(0)).getAllDeletedFiles().size());
      Assert.assertEquals(1, unseqTasks.get(0).getTotalPartialDeletedFilesNum());

      Assert.assertTrue(seqTasks.get(0).start());
      Assert.assertTrue(unseqTasks.get(0).start());
      Assert.assertEquals(--num, tsFileManager.getTsFileList(true).size());
      Assert.assertEquals(num, tsFileManager.getTsFileList(false).size());
    }

    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  @Test
  public void testSelectContinuousFilesBaseOnModsSizeWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, false);

    // the first file is all deleted, the rest file is partial deleted because its mods file size is
    // over threshold
    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());
    Assert.assertEquals(0, seqTasks.get(0).getAllDeletedFiles().size());
    Assert.assertEquals(0, unseqTasks.get(0).getAllDeletedFiles().size());
    Assert.assertEquals(2, seqTasks.get(0).getPartialDeletedFileGroups().size());
    Assert.assertEquals(5, seqTasks.get(0).getTotalPartialDeletedFilesNum());
    Assert.assertEquals(3, seqTasks.get(0).getPartialDeletedFileGroups().get(0).size());
    Assert.assertEquals(2, seqTasks.get(0).getPartialDeletedFileGroups().get(1).size());
    Assert.assertEquals(2, unseqTasks.get(0).getPartialDeletedFileGroups().size());
    Assert.assertEquals(5, unseqTasks.get(0).getTotalPartialDeletedFilesNum());
    Assert.assertEquals(3, unseqTasks.get(0).getPartialDeletedFileGroups().get(0).size());
    Assert.assertEquals(2, unseqTasks.get(0).getPartialDeletedFileGroups().get(1).size());

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());

    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
      Assert.assertFalse(tsFileManager.getTsFileList(false).get(i).getModFile().exists());
    }

    // select second time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  @Test
  public void testSelectContinuousFilesBaseOnDirtyRateByModsWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, false);

    // the first file is all deleted, the rest file is partial deleted because its mods file size is
    // over threshold
    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    addDevicedMods(5, seqResources, 0, 200);
    addDevicedMods(5, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());
    Assert.assertEquals(1, seqTasks.get(0).getAllDeletedFiles().size());
    Assert.assertEquals(1, unseqTasks.get(0).getAllDeletedFiles().size());
    Assert.assertEquals(2, seqTasks.get(0).getPartialDeletedFileGroups().size());
    Assert.assertEquals(4, seqTasks.get(0).getTotalPartialDeletedFilesNum());
    Assert.assertEquals(3, seqTasks.get(0).getPartialDeletedFileGroups().get(0).size());
    Assert.assertEquals(1, seqTasks.get(0).getPartialDeletedFileGroups().get(1).size());
    Assert.assertEquals(2, unseqTasks.get(0).getPartialDeletedFileGroups().size());
    Assert.assertEquals(4, unseqTasks.get(0).getTotalPartialDeletedFilesNum());
    Assert.assertEquals(3, unseqTasks.get(0).getPartialDeletedFileGroups().get(0).size());
    Assert.assertEquals(1, unseqTasks.get(0).getPartialDeletedFileGroups().get(1).size());

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());

    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
      Assert.assertFalse(tsFileManager.getTsFileList(false).get(i).getModFile().exists());
    }

    // select second time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  // base on dirty data rate
  @Test
  public void testSelectContinuousFileBaseOnDirtyDataRateWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setLongestExpiredTime(Long.MAX_VALUE);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, false);

    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1", 100);
    generateModsFile(1, 10, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(1, 10, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());

    // select second time
    // add device mods
    for (TsFileResource resource : seqResources) {
      resource
          .getModFile()
          .write(
              new Deletion(
                  new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0.**"),
                  Long.MAX_VALUE,
                  Long.MIN_VALUE,
                  Long.MAX_VALUE));
    }
    for (TsFileResource resource : unseqResources) {
      resource
          .getModFile()
          .write(
              new Deletion(
                  new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0.**"),
                  Long.MAX_VALUE,
                  Long.MIN_VALUE,
                  Long.MAX_VALUE));
    }
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());

    Assert.assertEquals(1, seqTasks.get(0).getPartialDeletedFileGroups().size());
    Assert.assertEquals(0, seqTasks.get(0).getAllDeletedFiles().size());
    Assert.assertEquals(5, seqTasks.get(0).getPartialDeletedFileGroups().get(0).size());
    Assert.assertEquals(1, unseqTasks.get(0).getPartialDeletedFileGroups().size());
    Assert.assertEquals(0, unseqTasks.get(0).getAllDeletedFiles().size());
    Assert.assertEquals(5, unseqTasks.get(0).getPartialDeletedFileGroups().get(0).size());

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());

    Assert.assertFalse(tsFileManager.getTsFileList(true).get(0).getModFile().exists());
    Assert.assertFalse(tsFileManager.getTsFileList(false).get(0).getModFile().exists());

    // select third time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  // base on outdated too long
  @Test
  public void testSelectContinuousFileBaseOnDirtyDataOutdatedTooLongWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setLongestExpiredTime(Long.MAX_VALUE);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, false);

    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1", 100);
    generateModsFile(1, 10, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(1, 10, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    // inner task, continuous
    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());

    // select second time
    // add the longest expired time
    IoTDBDescriptor.getInstance().getConfig().setLongestExpiredTime(1000);
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());

    Assert.assertEquals(2, seqTasks.get(0).getPartialDeletedFileGroups().size());
    Assert.assertEquals(0, seqTasks.get(0).getAllDeletedFiles().size());
    Assert.assertEquals(3, seqTasks.get(0).getPartialDeletedFileGroups().get(0).size());
    Assert.assertEquals(2, seqTasks.get(0).getPartialDeletedFileGroups().get(1).size());
    Assert.assertEquals(2, unseqTasks.get(0).getPartialDeletedFileGroups().size());
    Assert.assertEquals(0, unseqTasks.get(0).getAllDeletedFiles().size());
    Assert.assertEquals(3, unseqTasks.get(0).getPartialDeletedFileGroups().get(0).size());
    Assert.assertEquals(2, unseqTasks.get(0).getPartialDeletedFileGroups().get(1).size());

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());

    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
      Assert.assertFalse(tsFileManager.getTsFileList(false).get(i).getModFile().exists());
    }

    // select third time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  // base on dirty data rate
  @Test
  public void testSelectUncontinuousFileBaseOnDirtyDataRateWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setLongestExpiredTime(Long.MAX_VALUE);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, false, true);

    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // add one device ttl
    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0", 100);

    // select first time
    // add device mods with already outdated device
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 1) {
        seqResources
            .get(i)
            .getModFile()
            .write(
                new Deletion(
                    new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0.**"),
                    Long.MAX_VALUE,
                    Long.MIN_VALUE,
                    Long.MAX_VALUE));
      }
    }
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(0, seqTasks.size());

    // select second time
    // add device mods on file 0 2 4 6 8
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        for (int d = 1; d < 5; d++) {
          seqResources
              .get(i)
              .getModFile()
              .write(
                  new Deletion(
                      new PartialPath(
                          COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + d + ".**"),
                      Long.MAX_VALUE,
                      Long.MIN_VALUE,
                      Long.MAX_VALUE));
        }
      }
    }
    seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(0, seqTasks.get(0).getPartialDeletedFileGroups().size());
    Assert.assertEquals(5, seqTasks.get(0).getAllDeletedFiles().size());

    // select third time
    // add device mods
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 1) {
        seqResources
            .get(i)
            .getModFile()
            .write(
                new Deletion(
                    new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1.**"),
                    Long.MAX_VALUE,
                    Long.MIN_VALUE,
                    Long.MAX_VALUE));
      }
    }
    seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(5, seqTasks.get(0).getAllDeletedFiles().size());
    Assert.assertEquals(2, seqTasks.get(0).getPartialDeletedFileGroups().size());
    Assert.assertEquals(3, seqTasks.get(0).getPartialDeletedFileGroups().get(0).size());
    Assert.assertEquals(2, seqTasks.get(0).getPartialDeletedFileGroups().get(1).size());
    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
  }

  /**
   * File 0 2 4 6 8 is partial_deleted, file 1 is all_deleted. Partial_deleted group is (0 2) (4)
   * (6) (8).
   */
  @Test
  public void testSelectFileBaseOnDirtyDataRateWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setLongestExpiredTime(Long.MAX_VALUE);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, false, true);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // select first time
    // file 0 2 4 6 8 is partial_deleted, file 1 is all_deleted
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        seqResources
            .get(i)
            .getModFile()
            .write(
                new Deletion(
                    new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0.**"),
                    Long.MAX_VALUE,
                    Long.MIN_VALUE,
                    Long.MAX_VALUE));
        seqResources
            .get(i)
            .getModFile()
            .write(
                new Deletion(
                    new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1.**"),
                    Long.MAX_VALUE,
                    Long.MIN_VALUE,
                    Long.MAX_VALUE));
      }
    }
    for (int d = 0; d < 5; d++) {
      seqResources
          .get(1)
          .getModFile()
          .write(
              new Deletion(
                  new PartialPath(
                      COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + d + ".**"),
                  Long.MAX_VALUE,
                  Long.MIN_VALUE,
                  Long.MAX_VALUE));
    }
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, seqTasks.get(0).getAllDeletedFiles().size());
    Assert.assertEquals(4, seqTasks.get(0).getPartialDeletedFileGroups().size());
    Assert.assertEquals(2, seqTasks.get(0).getPartialDeletedFileGroups().get(0).size());
    Assert.assertEquals(1, seqTasks.get(0).getPartialDeletedFileGroups().get(1).size());
    Assert.assertEquals(1, seqTasks.get(0).getPartialDeletedFileGroups().get(2).size());
    Assert.assertEquals(1, seqTasks.get(0).getPartialDeletedFileGroups().get(3).size());

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertEquals(8, tsFileManager.getTsFileList(true).size());
  }

  /**
   * File 0 2 4 6 8 is partial_deleted, file 2 3 7 is all_deleted. Partial_deleted group is (0) (4)
   * (6 8).
   */
  @Test
  public void testSelectFileBaseOnDirtyDataRateWithHeavySelect2()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setLongestExpiredTime(Long.MAX_VALUE);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, false, true);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // select first time
    // file 0 2 4 6 8 is partial_deleted, file 2 3 7 is all_deleted
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        seqResources
            .get(i)
            .getModFile()
            .write(
                new Deletion(
                    new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0.**"),
                    Long.MAX_VALUE,
                    Long.MIN_VALUE,
                    Long.MAX_VALUE));
        seqResources
            .get(i)
            .getModFile()
            .write(
                new Deletion(
                    new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1.**"),
                    Long.MAX_VALUE,
                    Long.MIN_VALUE,
                    Long.MAX_VALUE));
      }
    }
    for (int d = 0; d < 5; d++) {
      seqResources
          .get(2)
          .getModFile()
          .write(
              new Deletion(
                  new PartialPath(
                      COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + d + ".**"),
                  Long.MAX_VALUE,
                  Long.MIN_VALUE,
                  Long.MAX_VALUE));
      seqResources
          .get(3)
          .getModFile()
          .write(
              new Deletion(
                  new PartialPath(
                      COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + d + ".**"),
                  Long.MAX_VALUE,
                  Long.MIN_VALUE,
                  Long.MAX_VALUE));
      seqResources
          .get(7)
          .getModFile()
          .write(
              new Deletion(
                  new PartialPath(
                      COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + d + ".**"),
                  Long.MAX_VALUE,
                  Long.MIN_VALUE,
                  Long.MAX_VALUE));
    }
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(3, seqTasks.get(0).getAllDeletedFiles().size());
    Assert.assertEquals(3, seqTasks.get(0).getPartialDeletedFileGroups().size());
    Assert.assertEquals(1, seqTasks.get(0).getPartialDeletedFileGroups().get(0).size());
    Assert.assertEquals(1, seqTasks.get(0).getPartialDeletedFileGroups().get(1).size());
    Assert.assertEquals(2, seqTasks.get(0).getPartialDeletedFileGroups().get(2).size());

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertEquals(6, tsFileManager.getTsFileList(true).size());
  }

  public void addDevicedMods(
      int deviceNum, List<TsFileResource> resources, long startTime, long endTime)
      throws IllegalPathException, IOException {
    for (int i = 0; i < deviceNum; i++) {
      for (TsFileResource resource : resources) {
        resource
            .getModFile()
            .write(
                new Deletion(
                    new PartialPath(
                        COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + i + ".**"),
                    Long.MAX_VALUE,
                    startTime,
                    endTime));
      }
    }
  }
}
