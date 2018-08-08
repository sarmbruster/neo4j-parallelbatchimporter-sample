package org.neo4j.sample.batchimporter;

import org.neo4j.unsafe.impl.batchimport.RelationshipImporter;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

import java.io.IOException;
import java.util.PrimitiveIterator;
import java.util.SplittableRandom;
import java.util.stream.LongStream;

public class RandomRelationshipInputChunk implements InputChunk {

    private final String REL_TYPE = "RELATED_TO";
    private final LongStream randomIds;
    private final PrimitiveIterator.OfLong randomIdIterator;
    private int count = 0;

    public RandomRelationshipInputChunk(int numberOfNodes, int numberOfRelationships) {
        this.randomIds = new SplittableRandom().longs(numberOfRelationships * 2, 0, numberOfNodes + 1);
        this.randomIdIterator = randomIds.iterator();
    }

    @Override
    public boolean next(InputEntityVisitor visitor) throws IOException {
        RelationshipImporter relationshipImporter = (RelationshipImporter) visitor;
        if (randomIdIterator.hasNext()) {
            relationshipImporter.startId(randomIdIterator.nextLong());
            relationshipImporter.endId(randomIdIterator.nextLong());
            relationshipImporter.type(REL_TYPE);
            relationshipImporter.endOfEntity();
            count++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() throws IOException {

    }
}
