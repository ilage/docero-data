package org.docero.data.processor;

import com.google.googlejavaformat.java.Formatter;
import com.google.googlejavaformat.java.FormatterException;
import com.google.googlejavaformat.java.JavaFormatterOptions;

import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.JavaFileObject;
import java.io.*;

public class JavaClassWriter implements Closeable {
    private final Writer writer;
    private StringBuffer notFormattedDataForWrite = new StringBuffer();

    public JavaClassWriter(ProcessingEnvironment environment, String fullPath) throws IOException {
        JavaFileObject sourceFile = environment.getFiler().createSourceFile(fullPath);
        writer = sourceFile.openWriter();
    }

    @Override
    public void close() throws IOException {
        String formattedFile;
        JavaFormatterOptions.Builder builder = JavaFormatterOptions.builder().style(JavaFormatterOptions.Style.AOSP);
        try {
            formattedFile = new Formatter(builder.build()).formatSourceAndFixImports(notFormattedDataForWrite.toString());
            writer.write(formattedFile);
        } catch (FormatterException e) {
            writer.write(String.valueOf(notFormattedDataForWrite));
            e.printStackTrace();
        }
        writer.flush();
        writer.close();

    }

    public void print(String s){
        notFormattedDataForWrite.append(s);
    }

    public void println(String s) {
        notFormattedDataForWrite.append(s+'\n');
    }


}
