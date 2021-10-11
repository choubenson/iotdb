package org.apache.iotdb.db.tools;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepairTool {
  private static final Logger logger = LoggerFactory.getLogger(RepairTool.class);

  public static void main(String[] args) {
    List<TsFileResource> oldResources = new ArrayList<>();
    List<File> tsFiles = checkArgs(args);
    for (File file : tsFiles) {
      TsFileResource resource = new TsFileResource(file);
      resource.setClosed(true);
      oldResources.add(resource);
    }
    for (TsFileResource oldResource : oldResources) {
      List<TsFileResource> newResources = new ArrayList<>();
      try (TsFileRewriteTool tsFileRewriteTool = new TsFileRewriteTool(oldResource)) {
        tsFileRewriteTool.parseAndRewriteFile(newResources);
      } catch (IOException | WriteProcessException | IllegalPathException e) {
        e.printStackTrace();
      }
    }
  }

  public static List<File> checkArgs(String[] args) {
    List<File> files = new ArrayList<>();
    if (args.length == 0) {
      return null;
    } else {
      for (String arg : args) {
        if (arg.endsWith(TSFILE_SUFFIX)) { // it's a file
          File f = new File(arg);
          if (!f.exists()) {
            continue;
          }
          files.add(f);
        } else { // it's a dir
          List<File> tmpFiles = getAllFilesInOneDirBySuffix(arg, TSFILE_SUFFIX);
          files.addAll(tmpFiles);
        }
      }
    }
    return files;
  }

  private static List<File> getAllFilesInOneDirBySuffix(String dirPath, String suffix) {
    File dir = new File(dirPath);
    if (!dir.isDirectory()) {
      logger.warn("It's not a directory path : " + dirPath);
      return Collections.emptyList();
    }
    if (!dir.exists()) {
      logger.warn("Cannot find Directory : " + dirPath);
      return Collections.emptyList();
    }
    List<File> tsFiles =
        new ArrayList<>(
            Arrays.asList(FSFactoryProducer.getFSFactory().listFilesBySuffix(dirPath, suffix)));
    File[] tmpFiles = dir.listFiles();
    if (tmpFiles != null) {
      for (File f : tmpFiles) {
        if (f.isDirectory()) {
          tsFiles.addAll(getAllFilesInOneDirBySuffix(f.getAbsolutePath(), suffix));
        }
      }
    }
    return tsFiles;
  }
}
