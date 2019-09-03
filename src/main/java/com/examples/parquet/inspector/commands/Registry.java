package com.examples.parquet.inspector.commands;

import com.examples.parquet.inspector.exceptions.CommandNotFoundException;

import java.util.HashMap;
import java.util.Map;

public class Registry {
    public static Map<String, Class> registry = new HashMap<>();

    static {
        registry.put("schema", SchemaCommand.class);
        registry.put("meta", MetaCommand.class);
        registry.put("cat", CatCommand.class);
    }

    public static Command getCommandByName(String name) throws CommandNotFoundException, IllegalAccessException, InstantiationException {

        Class<? extends Command> clazz = registry.get(name);
        if (clazz == null) {
            throw new CommandNotFoundException("Command not found!");
        }
        return clazz.newInstance();

    }

}
