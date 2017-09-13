package org.docero.data.processor;

import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.JavaFileObject;
import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;

public class JavaClassWriter implements Closeable {
    private final JavaFileObject sourceFile;
    private final Writer writer;
    private static final String blockOffset = "    ";

    private int blockIndent = 0;
    private boolean lineStart = true;

    public JavaClassWriter(ProcessingEnvironment environment, String fullPath) throws IOException {
        sourceFile = environment.getFiler().createSourceFile(fullPath);
        writer = sourceFile.openWriter();
    }

    @Override
    public void close() throws IOException {
        writer.flush();
        writer.close();
    }

    public void print(String s) throws IOException {
        for (int i = 0; lineStart && i < blockIndent; i++)
            writer.write(blockOffset);
        writer.write(s);
        lineStart = s.charAt(s.length() - 1) == '\n';
    }

    public void println(String s) throws IOException {
        if (s != null && s.length() > 0) {
            for (int i = 0; lineStart && i < blockIndent; i++)
                writer.write(blockOffset);
            writer.write(s);
        }
        writer.write("\n");
        lineStart = true;
    }

    public void startBlock(String s) throws IOException {
        this.println(s);
        blockIndent++;
    }

    public void endBlock(String s) throws IOException {
        blockIndent--;
        this.println(s);
    }
}
