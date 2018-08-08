package org.neo4j.sample.batchimporter;

import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;

import java.io.IOException;

public class SingleChunkInputIterator extends InputIterator.Adapter {

    private boolean alreadyExhausted = false;
    private final InputChunk inputChunk;

    public SingleChunkInputIterator(InputChunk inputChunk) {
        this.inputChunk = inputChunk;
    }

    @Override
    public InputChunk newChunk() {
        return alreadyExhausted ? null : inputChunk;
    }

    @Override
    public boolean next(InputChunk chunk) throws IOException {
        if (alreadyExhausted) {
            return false;
        } else {
            alreadyExhausted = true;
            return true;
        }
    }
}
