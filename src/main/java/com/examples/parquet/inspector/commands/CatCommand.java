package com.examples.parquet.inspector.commands;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public class CatCommand implements Command {

    @Override
    public void execute(String[] args) {

        Path path = new Path(args[0]);

        Configuration conf = new Configuration();

        try {
            HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
            ParquetFileReader r = ParquetFileReader.open(inputFile);
            MessageType schema = r.getFileMetaData().getSchema();

            PageReadStore rowGroup;
            int rowGroupIndex = 0;
            try {
                while (null != (rowGroup = r.readNextRowGroup())) {
                    final long rows = rowGroup.getRowCount();
                    System.out.println("Number of rows: " + rows + " in rowGroup " + rowGroupIndex);

                    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                    final RecordReader recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));

                    for (int i = 0; i < rows; i++) {
                        System.out.println("Row Group Index: " + rowGroupIndex + ", Row Index: " + i);
                        System.out.println(recordReader.read());
                    }
                    rowGroupIndex++;
                }
            } finally {
                r.close();
            }
        } catch (IOException e) {
            System.out.println("Error reading parquet file.");
            e.printStackTrace();
        }

    }


}
