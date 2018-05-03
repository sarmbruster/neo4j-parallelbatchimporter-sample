package org.neo4j.sample.batchimporter;

import org.neo4j.unsafe.impl.batchimport.NodeImporter;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

import java.io.IOException;

public class NodeInputChunk implements InputChunk {

    private int sent = 0;

    @Override
    public boolean next(InputEntityVisitor visitor) throws IOException {

        if (visitor instanceof NodeImporter) {
            visitor.id(sent);
            visitor.endOfEntity();
            return sent++ < 10;
        } else {
            return false;
        }

    }

    @Override
    public void close() throws IOException {

    }
}
