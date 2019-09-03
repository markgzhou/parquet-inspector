package com.examples.parquet.inspector;

import com.examples.parquet.inspector.commands.Command;
import com.examples.parquet.inspector.commands.Registry;
import com.examples.parquet.inspector.exceptions.CommandNotFoundException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Arrays;

import static java.lang.System.exit;


public class Main {

    public static void main(String[] args) throws InstantiationException, IllegalAccessException {

        //Turn off warning logs from Hadoop libraries
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        String commandName = args[0];
        Command command = null;
        try {
            command = Registry.getCommandByName(commandName);
        } catch (CommandNotFoundException e) {
            System.out.println(e.getMessage());
            System.out.println("Please use valid commands: cat, meta, schema.");
            System.out.println("Usage: java -jar parquet-inspector-all.jar [command] [file]");
            System.out.println("Example: java -jar parquet-inspector-all.jar meta /Data/parquet_file.parquet");
            exit(-1);
        }

        String[] argsWithoutCommand = null;
        if (args.length > 1) {
            argsWithoutCommand = Arrays.copyOfRange(args, 1, args.length);
        }

        try {
            command.execute(argsWithoutCommand);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
