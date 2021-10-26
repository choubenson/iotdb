package org.apache.iotdb.tsfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class TsFileReadVector {

  public static void main(String[] args) throws IOException, IllegalPathException {
    String path = "C:\\IOTDB\\sourceCode\\annotation\\iotdb\\test.tsfile";
    String device = Constant.DEVICE_PREFIX + 1;
    String sensorPrefix = "sensor_";
    String vectorName = "vector1";

    try (TsFileSequenceReader reader = new TsFileSequenceReader(path);
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader)) {
      List<String> fullVectorPaths=new ArrayList<>();
      for(int i=0;i<10;i++){
        fullVectorPaths.add(device+"."+vectorName+"."+sensorPrefix+(i+1));
      }
      List<Path> vectorPaths=new ArrayList<>();
      vectorPaths.add(new VectorPartialPath(new Path(device,vectorName).getFullPath(),fullVectorPaths));
      QueryExpression queryExpression=QueryExpression.create(vectorPaths,null);
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      System.out.println(queryDataSet);
    }
  }

}
