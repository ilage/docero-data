package org.docero.data.tests;

import org.apache.ibatis.session.Configuration;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class MyBatisSpringConfigurationFactory implements ApplicationContextAware {

    private static class AplicationContextHolder{

        private static final ContextResource CONTEXT_PROV = new ContextResource();

        private AplicationContextHolder() {
            super();
        }
    }

    private static final class ContextResource {

        private ApplicationContext context;

        private ContextResource(){
            super();
        }

        private void setContext(ApplicationContext context){
            this.context = context;
        }
    }

    private static ApplicationContext getApplicationContext() {
        return AplicationContextHolder.CONTEXT_PROV.context;
    }

    @Override
    public void setApplicationContext(ApplicationContext ac) {
        AplicationContextHolder.CONTEXT_PROV.setContext(ac);
    }

    public static Configuration getConfiguration() throws Exception {
        SqlSessionFactoryBean ssfb = getApplicationContext().getBean(SqlSessionFactoryBean.class);
        return ssfb.getObject().getConfiguration();
    }
}
