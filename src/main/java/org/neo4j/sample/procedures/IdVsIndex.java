package org.neo4j.sample.procedures;

import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.SplittableRandom;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class IdVsIndex {

    private static final Label LABEL = Label.label("Document");
    @Context
    public GraphDatabaseService graphDatabaseService;

    @Procedure(name = "idvsindex.id")
    public Stream<DurationResult> idLookup(@Name("numer_of_lookups") Long numberOfLookups) {

        long numberOfNodes = Iterators.single(graphDatabaseService.execute("MATCH (n) RETURN COUNT(*) AS c").columnAs("c"));
        PrimitiveIterator.OfLong randomValueIterator = new SplittableRandom()
                .longs(numberOfLookups, 0, numberOfNodes + 1).iterator();

        return withStopWatch(aVoid -> {
            while (randomValueIterator.hasNext()) {
                Node node = graphDatabaseService.getNodeById(randomValueIterator.nextLong());
            }
        });
    }

    @Procedure(name = "idvsindex.index")
    public Stream<DurationResult> indexLookup(@Name("numer_of_lookups") Long numberOfLookups) {

        // preparation: fetch randomly numberOfLookups valid uuids from graph
        int numberOfLookupsInt = Math.toIntExact(numberOfLookups);
        List<String> uuids = new ArrayList<>(numberOfLookupsInt);

        long numberOfNodes = Iterators.single(graphDatabaseService.execute("MATCH (n) RETURN COUNT(*) AS c").columnAs("c"));
        PrimitiveIterator.OfLong randomValueIterator = new SplittableRandom()
                .longs(numberOfLookups, 0, numberOfNodes + 1).iterator();

        while (randomValueIterator.hasNext()) {
            Node node = graphDatabaseService.getNodeById(randomValueIterator.nextLong());
            uuids.add((String) node.getProperty("uuid"));
        }

        return withStopWatch(aVoid -> {

            for (String uuid : uuids) {
                Node node = graphDatabaseService.findNode(LABEL, "uuid", uuid);
            }

        });
    }

    private Stream<DurationResult> withStopWatch(Consumer<Void> action ) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        action.accept(null);

        stopWatch.stop();
        return Stream.of(new DurationResult(stopWatch.getTime()));
    }

    public static class DurationResult {
        public Long duration;

        public DurationResult(Long duration) {
            this.duration = duration;
        }
    }
}
