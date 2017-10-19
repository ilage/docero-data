package org.docero.data.processor;

import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import java.io.IOException;
import java.util.*;

@SuppressWarnings("WeakerAccess")
class BatchRepositoryBuilder {
    final List<TypeMirror> beans;
    final TypeMirror repositoryInterface;
    final DDataBuilder dataBuilder;
    final String implClassName;

    BatchRepositoryBuilder(DDataBuilder dDataBuilder, TypeElement repositoryElement) {
        dataBuilder = dDataBuilder;
        repositoryInterface = repositoryElement.asType();
        implClassName = repositoryInterface + "_Impl_";
        beans = new ArrayList<>();
        Optional<Object> beansOpt = repositoryElement.getAnnotationMirrors().stream()
                .filter(m -> m.getAnnotationType().toString().endsWith("DDataRep"))
                .findAny()
                .map(m -> m.getElementValues().entrySet().stream()
                        .filter(e -> e.getKey().toString().equals("beans()"))
                        .findAny().map(e -> e.getValue().getValue()).orElse(""));
        if (beansOpt.isPresent()) {
            List b = beansOpt.get() instanceof List ? (List) beansOpt.get() : Collections.singletonList(beansOpt.get());
            for (Object v : b) {
                String classValue = v.toString();
                beans.add(dataBuilder.environment.getElementUtils()
                        .getTypeElement(classValue.substring(0, classValue.lastIndexOf('.'))).asType());
            }
        }
    }

    void createSpringBean(JavaClassWriter cf) throws IOException {
        String daoInterfaceName = repositoryInterface.toString();
        int offset = daoInterfaceName.lastIndexOf('.') + 1;
        String methodName = Character.toLowerCase(daoInterfaceName.charAt(offset)) +
                daoInterfaceName.substring(offset + 1);
        cf.println("@org.springframework.context.annotation.Bean");
        cf.startBlock("public " + daoInterfaceName + " " + methodName + "(");
        cf.println("org.apache.ibatis.session.SqlSessionFactory sqlSessionFactory");
        cf.endBlock(")");
        cf.startBlock("{");
        cf.startBlock("return new " + implClassName + "(");
        cf.println("sqlSessionFactory");
        cf.endBlock(");");
        cf.endBlock("}");
    }

    private class BeanData {
        final DataBeanBuilder bean;
        final DataRepositoryBuilder repository;
        final String repositoryVariable;

        private BeanData(DataBeanBuilder bean, DataRepositoryBuilder repository) {
            this.bean = bean;
            this.repository = repository;
            String str = repository.mappingClassName.substring(repository.mappingClassName.lastIndexOf('.') + 1);
            repositoryVariable = Character.toLowerCase(str.charAt(0)) + str.substring(1) +
                    (repository.isCreatedByInterface ? "Repository" : "");
        }
    }

    void generate() throws IOException {
        int simpNameDel = implClassName.lastIndexOf('.');

        HashMap<TypeMirror, BeanData> supported = new HashMap<>();
        for (TypeMirror bean : beans) {
            DataBeanBuilder beanBuilder = dataBuilder.beansByInterface.get(bean.toString());
            DataRepositoryBuilder beanRepository = dataBuilder.repositoriesByBean.get(bean.toString());
            supported.put(bean, new BeanData(beanBuilder, beanRepository));
        }

        try (JavaClassWriter cf = new JavaClassWriter(dataBuilder.environment, implClassName)) {
            String implPackage = implClassName.substring(0, simpNameDel);
            cf.println("package " + implPackage + ";");

            for (BeanData d : supported.values()) {
                String beanPkg = d.bean.interfaceType.toString();
                beanPkg = beanPkg.substring(0, beanPkg.lastIndexOf('.'));
                if (!implPackage.equals(beanPkg))
                    cf.println("import " + beanPkg + ".*;");
            }
            cf.println("import org.apache.ibatis.session.SqlSessionFactory;");
            cf.println("import org.apache.ibatis.session.ExecutorType;");
            if (dataBuilder.spring) cf.println("import org.mybatis.spring.SqlSessionTemplate;");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            cf.startBlock("public final class " +
                    implClassName.substring(simpNameDel + 1)
                    + (dataBuilder.spring ? " extends org.mybatis.spring.support.SqlSessionDaoSupport" : "") +
                    " implements " + repositoryInterface + " {");
            if (dataBuilder.spring) {
                for (BeanData d : supported.values()) {
                    cf.println("private final org.docero.data.DDataRepository<" +
                            d.bean.interfaceType + "," +
                            d.bean.keyType + "> " + d.repositoryVariable + ";");
                }
                cf.println("");
                cf.startBlock("public " + implClassName.substring(simpNameDel + 1) + "(");
                cf.println("SqlSessionFactory sqlSessionFactory");
                cf.endBlock(")");
                cf.startBlock("{");
                cf.println("this.setSqlSessionFactory(sqlSessionFactory);");
                for (BeanData d : supported.values()) {
                    cf.println("this." + d.repositoryVariable + " = org.docero.data.DData.getRepository(" + d.bean.interfaceType + ".class);");
                    cf.println("((org.mybatis.spring.support.SqlSessionDaoSupport)" + d.repositoryVariable +
                            ").setSqlSessionTemplate((SqlSessionTemplate)this.getSqlSession());");
                }
                cf.endBlock("}");

                cf.println("");
                cf.startBlock("@Override public void setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {");
                cf.println("super.setSqlSessionTemplate(new SqlSessionTemplate(sqlSessionFactory, ExecutorType.BATCH));");
                cf.endBlock("}");
                cf.println("");
                cf.startBlock("@Override public void setSqlSessionTemplate(SqlSessionTemplate sqlSessionTemplate) {");
                cf.println("if (sqlSessionTemplate.getExecutorType()==ExecutorType.BATCH) " +
                        "super.setSqlSessionTemplate(sqlSessionTemplate);");
                cf.println("else throw new IllegalArgumentException(\"invalid sqlSessionTemplate for BATCH operations\");");
                cf.endBlock("}");
            }
            cf.println("");
            cf.startBlock("@Override public <T extends java.io.Serializable> T create(Class<T> clazz) {");
            cf.println("if(clazz == null) return null;");
            for (BeanData d : supported.values()) {
                cf.println("if (clazz == " + d.bean.interfaceType + ".class) " +
                        "return (T) new " + d.bean.getImplementationName() + "();");
            }
            cf.println("throw new IllegalArgumentException(\"unknown class for repository: \"+clazz.getCanonicalName());");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("@Override public <T extends java.io.Serializable> T get(Class<T> clazz, java.io.Serializable id) {");
            cf.println("if(clazz == null || id == null) return null;");
            for (BeanData d : supported.values()) {
                cf.println("if (clazz == " + d.bean.interfaceType + ".class) " +
                        "return (T) " + d.repositoryVariable + ".get((" + d.bean.keyType + ")id);");
            }
            cf.println("throw new IllegalArgumentException(\"unknown class for repository: \"+clazz.getCanonicalName());");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("@Override public java.util.List<org.apache.ibatis.executor.BatchResult> flushStatements() {");
            cf.println("return super.getSqlSession().flushStatements();");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("@Override public void insert(java.io.Serializable bean) {");
            cf.println("if(bean == null) return;");
            for (BeanData d : supported.values()) {
                cf.println("else if (bean instanceof " + d.bean.interfaceType + ") " +
                        "" + d.repositoryVariable + ".insert((" + d.bean.interfaceType + ")bean);");
            }
            cf.println("else throw new IllegalArgumentException(\"unknown class for repository: \"+bean.getClass().getCanonicalName());");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("@Override public void update(java.io.Serializable bean) {");
            cf.println("if(bean == null) return;");
            for (BeanData d : supported.values()) {
                cf.println("else if (bean instanceof " + d.bean.interfaceType + ") " +
                        "" + d.repositoryVariable + ".update((" + d.bean.interfaceType + ")bean);");
            }
            cf.println("else throw new IllegalArgumentException(\"unknown class for repository: \"+bean.getClass().getCanonicalName());");
            cf.endBlock("}");

            cf.endBlock("}");
        }
    }
}
