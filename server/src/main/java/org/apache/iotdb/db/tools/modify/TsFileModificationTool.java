package org.apache.iotdb.db.tools.modify;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.tools.TsFileRewriteTool;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TsFileModificationTool extends TsFileRewriteTool {

  private static final Logger logger = LoggerFactory.getLogger(TsFileModificationTool.class);
  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public TsFileModificationTool(TsFileResource resourceToBeModified) throws IOException {
    super(resourceToBeModified);
  }

  public TsFileModificationTool(TsFileResource resourceToBeModified, boolean needReaderForV2)
      throws IOException {
    super(resourceToBeModified, needReaderForV2);
  }

  public static void main(String[] args) {
    File[] tsFiles = checkArgs(args);
    List<TsFileResource> oldTsFileResources = new ArrayList<>();
    for (File file : tsFiles) {
      oldTsFileResources.add(new TsFileResource(file));
    }
    try {
      modifyTsFiles(oldTsFileResources);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
  }

  public static File[] checkArgs(String[] args) { // 根据路径名获得其下存在的所有TsFile文件
    List<String> filePaths = new ArrayList<>();
    String filePath = "test.tsfile";
    if (args.length == 1) {
      filePath = args[0];
    } else {
      logger.error("Uncorrect args");
      return null;
    }
    return FSFactoryProducer.getFSFactory().listFilesBySuffix(filePath, TSFILE_SUFFIX);
  }

  /**
   * @param resourcesToBeModified
   * @return
   */
  public static List<TsFileResource> modifyTsFiles( // 返回的是新TsFile的TsFileResource
      List<TsFileResource> resourcesToBeModified)
      throws IOException, WriteProcessException { // 参数是每个待整理的旧TsFile的TsFileResource
    List<TsFileResource> newTsFileResources =
        new ArrayList<>(); // 每个旧TsFile对应的新TsFile的TsFileResource，此处一个旧TsFile升级后对应一个新TsFile(两个TsFile是同一时间分区的)
    List<TsFileResource> modifiedResources = new ArrayList<>();
    for (TsFileResource resourceToBeModified : resourcesToBeModified) {
      try (TsFileModificationTool modifier =
          new TsFileModificationTool(resourceToBeModified)) { // 每次执行完try里的内容会自动释放掉modifier占用的资源
        modifier.modifyTsFile(modifiedResources);
        newTsFileResources.add(modifiedResources.get(0));
      }
    }
    return newTsFileResources;
  }

  private void modifyTsFile(List<TsFileResource> modifiedResources)
      throws IOException, WriteProcessException {
    try {
      parseAndRewriteFile(modifiedResources);
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
  }
}
