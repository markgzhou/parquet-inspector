package com.examples.parquet.inspector.commands;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.List;

public class MetaCommand implements Command {

    @Override
    public void execute(String[] args) throws Exception {

        Path path = new Path(args[0]);

        Configuration conf = new Configuration();
        HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);

        try {
            ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile);
            FileMetaData fileMetaData = parquetFileReader.getFileMetaData();
            List<BlockMetaData> blocks = parquetFileReader.getFooter().getBlocks();
            System.out.println(ParquetMetadata.toPrettyJSON(new ParquetMetadata(fileMetaData, blocks)));

        } catch (IOException e) {
            System.out.println("Error reading parquet schema.");
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Command");
        }

    }


}
