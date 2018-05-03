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
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.values.storable.Value;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.function.ToIntFunction;

import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors.defaultVisible;

public class Importer {

    public static void main(String[] args) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        File storeDir = Files.createTempDirectory("dummy").toFile();
        LogProvider logging = NullLogProvider.getInstance();
        JobScheduler jobScheduler = new CentralJobScheduler();


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

            importer.doImport(new Input() {
                @Override
                public InputIterable nodes() {
                    return null;
                }

                @Override
                public InputIterable relationships() {
                    return null;
                }

                @Override
                public IdMapper idMapper(NumberArrayFactory numberArrayFactory) {
                    return null;
                }

                @Override
                public Collector badCollector() {
                    return null;
                }

                @Override
                public Estimates calculateEstimates(ToIntFunction<Value[]> valueSizeCalculator) throws IOException {
                    return null;
                }
            });
        }
    }
}
