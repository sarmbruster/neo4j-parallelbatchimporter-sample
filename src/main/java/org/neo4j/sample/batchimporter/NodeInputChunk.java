package org.neo4j.sample.batchimporter;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import org.neo4j.unsafe.impl.batchimport.NodeImporter;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

import java.io.IOException;

public class NodeInputChunk implements InputChunk {

    public static final String[] LABELS = {"Document"};
    private final NoArgGenerator uuidGenerator;
    private int sent = 0;
    private final int maxNumberOfNodes;

    public NodeInputChunk(int numberOfNodes) {
        this.maxNumberOfNodes = numberOfNodes;
        this.uuidGenerator = Generators.timeBasedGenerator(EthernetAddress.fromInterface());
    }

    @Override
    public boolean next(InputEntityVisitor visitor) throws IOException {
        NodeImporter nodeImporter = (NodeImporter) visitor;
        nodeImporter.labels(LABELS);
        visitor.id(sent);
        visitor.property("uuid", uuidGenerator.generate().toString());
        visitor.endOfEntity();
        return sent++ < maxNumberOfNodes;
    }

    @Override
    public void close() {
    }
}
