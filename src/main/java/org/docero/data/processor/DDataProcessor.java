package org.docero.data.processor;

import org.docero.data.DDataBean;
import org.docero.data.DDataRep;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Процессор анотаций.
 * <p>Подключение в Maven: &lt;annotationProcessor&gt;org.docero.histdb.HistDbProcessor&lt;/annotationProcessor&gt;</p>
 * histDbEntityPath - пакет в котором генерировать классы сущностей
 * <p>в Maven: &lt;compilerArgument&gt;-AhistDbEntityPath=org.name.project.entities&lt;/compilerArgument&gt;</p>
 * Created by i.vasyashin on 01.09.2016.
 */
@SupportedAnnotationTypes({
        "org.docero.data.DDataBean",
        "org.docero.data.DDataRep"
})
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class DDataProcessor extends AbstractProcessor {
    private TypeMirror collectionType;
    private DDataBuilder builder;

    @Override
    public void init(ProcessingEnvironment environment) {
        super.init(environment);
        collectionType = environment.getTypeUtils().erasure(
                environment.getElementUtils().getTypeElement("java.util.Collection").asType()
        );
        builder = new DDataBuilder(environment);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        DDataMapBuilder.build(builder,this.processingEnv,collectionType);

        if (annotations.size() == 0) {
            return false;
        }

        Throwable error = null;
        try {
            Set<? extends Element> entities = roundEnv.getElementsAnnotatedWith(DDataBean.class);
            for (Element beanElement : entities) builder.checkInterface(beanElement, collectionType);

            Set<? extends Element> repositories = roundEnv.getElementsAnnotatedWith(DDataRep.class);
            for (Element repositoryElement : repositories) builder.checkRepository(repositoryElement);

        } catch (Exception e) {
            StringWriter err = new StringWriter();
            e.printStackTrace(new PrintWriter(err));
            error = e;
        }

        try {
            builder.generateClasses();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (error != null) throw new Error(error);

        return false;
    }
}
