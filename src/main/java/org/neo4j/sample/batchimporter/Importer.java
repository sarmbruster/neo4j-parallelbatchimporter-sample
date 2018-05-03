package org.neo4j.sample.batchimporter;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.tooling.ImportTool;
import org.neo4j.unsafe.impl.batchimport.*;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.BadCollector;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;

import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;

import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors.defaultVisible;

public class Importer {

    public static void main(String[] args) throws Throwable {

        File storeDir = Files.createTempDirectory("dummy").toFile();
        LogProvider logging = NullLogProvider.getInstance();
        JobScheduler jobScheduler = new CentralJobScheduler();
        jobScheduler.init();

        try (FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction()) {

            Configuration config = new Configuration() { // checkout source code of this class, contains useful comments

                /*@Override
                public long pageCacheMemory() {
                    return 0;
                }

                @Override
                public long maxMemoryUsage() {
                    return 0;
                }*/
            };
            Config dbConfig = Config.defaults();;
            BatchImporter importer = BatchImporterFactory.withHighestPriority().instantiate(
                    storeDir,
                    fileSystemAbstraction,
                    null,
                    config,
                    new SimpleLogService( logging, logging ),
                    defaultVisible( jobScheduler ),
                    AdditionalInitialIds.EMPTY,
                    dbConfig,
                    RecordFormatSelector.selectForConfig( dbConfig, logging ),
                    NO_MONITOR
            );

            Method printOverviewMethod = ImportTool.class.getDeclaredMethod("printOverview", File.class, Collection.class, Collection.class, Configuration.class, PrintStream.class);
            printOverviewMethod.setAccessible(true);
            printOverviewMethod.invoke(null, storeDir, Collections.emptyList(), Collections.emptyList(), config, System.out);

            Groups groups = new Groups();

            importer.doImport(Inputs.input(
                    InputIterable.replayable(() -> new SingleChunkInputIterator()),
                    InputIterable.replayable(() -> new SingleChunkInputIterator()),
                    IdMappers.strings(NumberArrayFactory.AUTO_WITHOUT_PAGECACHE, groups),
                    new BadCollector(System.err, 0, 0),  // TODO: provide reasonable numbers
                    Inputs.knownEstimates(-1, -1, -1, -1,-1, -1,-1) // TODO: provide reasonable estimations
            ));
        }
    }
}
