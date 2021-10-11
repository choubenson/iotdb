package org.apache.iotdb.db.service;

import org.apache.iotdb.db.engine.modify.ModifyTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ModifyService implements IService {
  private AtomicInteger threadCnt = new AtomicInteger(); // 线程数量，默认是0
  private ExecutorService modifyThreadPool;
  private List<String> tsFilePaths;

  public List<String> getTsFilePaths() {
    return tsFilePaths;
  }

  public void setTsFilePaths(List<String> tsFilePaths) {
    this.tsFilePaths = tsFilePaths;
  }

  public static ModifyService getINSTANCE() {
    return InstanceHolder.INSTANCE;
  }

  public static class InstanceHolder {
    private static final ModifyService INSTANCE = new ModifyService();

    private InstanceHolder() {}
  }

  @Override
  public void start() throws StartupException {
    int modifyThreadNum = 1;
    modifyThreadPool =
        Executors.newFixedThreadPool(
            modifyThreadNum,
            r -> new Thread(r, "UpgradeThread-" + threadCnt.getAndIncrement())); // 创建升级线程池

    for (String path : tsFilePaths) {
      moidfyOneTsFile(path);
    }
  }

  @Override
  public void stop() {}

  @Override
  public ServiceType getID() {
    return null;
  }

  private static void modifyTsFiles(List<String> filePaths) {
    moidfyOneTsFile(filePaths.get(0));
  }

  private static void moidfyOneTsFile(String filePath) {
    FSFactory fsFactory = FSFactoryProducer.getFSFactory();
    TsFileResource tsFileResource = new TsFileResource(fsFactory.getFile(filePath));
    tsFileResource.doModify();
  }

  public void submitModifyTask(ModifyTask modifyTask) { // 往该“整理TSFile文件”服务的线程池里提交该整理线程modifyTask
    modifyThreadPool.submit(modifyTask);
  }
}
