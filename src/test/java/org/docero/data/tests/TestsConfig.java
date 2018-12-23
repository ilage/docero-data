package org.docero.data.tests;

import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.TypeHandler;
import org.docero.data.rmt.RemoteBean;
import org.docero.data.rmt.RemoteRepository;
import org.docero.data.rmt.RemoteRepositoryImpl;
import org.docero.data.utils.DDataSpringResources;
import org.docero.data.utils.UUIDTypeHandler;
import org.docero.data.view.DDataViewBuilder;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.transaction.SpringManagedTransactionFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.docero.data.*;

import javax.sql.DataSource;
import java.util.Arrays;

@Configuration
@Import({DDataConfiguration.class})
@EnableTransactionManagement
@EnableCaching
@EnableDDataConfiguration(packageClass = TestsConfig.class)
public class TestsConfig {
    @Bean
    public DataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:postgresql://localhost:5432/test",
                "postgres", "postgres");
        dataSource.setDriverClassName("org.postgresql.Driver");
        return dataSource;
    }

    @Bean
    @Primary
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        DataSourceTransactionManager tm = new DataSourceTransactionManager();
        tm.setDataSource(dataSource);
        return tm;
    }

    @Bean
    public TransactionFactory transactionFactory() {
        return new SpringManagedTransactionFactory();
    }

    @Bean
    @Primary
    public SqlSessionFactoryBean sqlSessionFactoryBean(
            DataSource dataSource,
            TransactionFactory transactionManager,
            ApplicationContext context
    ) throws Exception {
        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
        bean.setConfiguration(new org.apache.ibatis.session.Configuration() {{
            this.setLazyLoadingEnabled(true);
            this.setAggressiveLazyLoading(false);
            this.setMultipleResultSetsEnabled(true);
            this.getLazyLoadTriggerMethods().clear();
            //this.getLazyLoadTriggerMethods().add("toString");
            this.setConfigurationFactory(MyBatisSpringConfigurationFactory.class);
        }});
        bean.setDataSource(dataSource);
        bean.setMapperLocations(DDataSpringResources.get(context, DDataModule.class));
        bean.setTransactionFactory(transactionManager);
        bean.setObjectFactory(DData.getObjectFactory());
        bean.setTypeHandlers(new TypeHandler[]{new UUIDTypeHandler()});
        new MyBatisSpringConfigurationFactory().setApplicationContext(context);

        return bean;
    }

    @Bean
    public DData dData(SqlSessionFactory sessionFactory) {
        return new DData(sessionFactory);
    }

    @Bean
    public DDataViewBuilder dataViewBuilder(DData dData) {
        return dData.getViewBuilder();
    }

    @DependsOn("dData")
    @Bean
    public RemoteRepository remoteRepository() {
        RemoteRepository remote = new RemoteRepositoryImpl();
        return DData.registerRemote(remote, RemoteBean.class);
    }

    @DependsOn("dData")
    @Bean
    CacheManager cacheManager() {
        return new ConcurrentMapCacheManager(DData.getCacheNames());
    }
}
