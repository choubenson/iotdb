package org.apache.iotdb.db.engine.modify;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.upgrade.UpgradeTask;
import org.apache.iotdb.db.tools.modify.TsFileModificationTool;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModifyTask extends WrappedRunnable { //整理TsFile文件线程
  private TsFileResource resourceToBeModified; //待升级旧的TsFile文件的TsFileResource
  private static final Logger logger = LoggerFactory.getLogger(UpgradeTask.class);
  private static final String COMMA_SEPERATOR = ",";

  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public ModifyTask(TsFileResource resourceToBeModified) {
    this.resourceToBeModified = resourceToBeModified;
  }



  @Override
  public void runMayThrow() throws Exception {
    //TsFileModificationTool.modifyTsFiles(resourceToBeModified);

  }
}
