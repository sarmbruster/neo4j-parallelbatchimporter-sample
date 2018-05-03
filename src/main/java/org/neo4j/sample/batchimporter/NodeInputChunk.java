package org.neo4j.sample.batchimporter;

import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

import java.io.IOException;

public class NodeInputChunk implements InputChunk {

    private int sent = 0;

    @Override
    public boolean next(InputEntityVisitor visitor) throws IOException {

        visitor.id(sent);
        return sent++ < 10;
    }

    @Override
    public void close() throws IOException {

    }
}
