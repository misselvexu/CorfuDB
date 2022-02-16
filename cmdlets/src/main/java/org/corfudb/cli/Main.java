package org.corfudb.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * Created by mwei on 11/18/16.
 */
public class Main {

    public static class Args {

        @Parameter(names = { "-log", "-verbose" }, description = "Level of verbosity")
        private Integer verbose = 1;

        @Parameter(names = "-groups", description = "Comma-separated list of group names to be run")
        private String groups;

        @Parameter(names = "-debug", description = "Debug mode")
        private boolean debug = false;
    }

    public static void main(String[] args) {
        Args args1 = new Args();
        JCommander.newBuilder()
                .addObject(args1)
                .build()
                .parse(args);
        System.out.println("hello world! " + args1.groups);
    }
}
