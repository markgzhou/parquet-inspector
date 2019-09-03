package com.examples.parquet.inspector.commands;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.List;

public class SchemaCommand implements Command {
    private static final String LINE_SEPARATOR = System.lineSeparator();

    @Override
    public void execute(String[] args) throws Exception {

        Path path = new Path(args[0]);

        Configuration conf = new Configuration();
        StringBuilder sb = new StringBuilder();
        HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);

        try {
            ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile);
            FileMetaData fileMetaData = parquetFileReader.getFileMetaData();
            MessageType schema = fileMetaData.getSchema();

            sb.append("Schema Type: ").append(schema.getName()).append(LINE_SEPARATOR);

            sb.append("Schemas:").append(LINE_SEPARATOR);
            List<Type> allFields = schema.getFields();
            for (Type t : allFields) {
                t.writeToStringBuilder(sb, "");
                sb.append(LINE_SEPARATOR);
            }

            System.out.println(sb.toString());

        } catch (IOException e) {
            System.out.println("Error reading parquet schema.");
            e.printStackTrace();
        }

    }


}
