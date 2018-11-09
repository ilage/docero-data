package org.docero.data.tests;

import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.TypeHandler;
import org.docero.data.rmt.RemoteBean;
import org.docero.data.rmt.RemoteRepository;
import org.docero.data.rmt.RemoteRepositoryImpl;
import org.docero.data.utils.UUIDTypeHandler;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.transaction.SpringManagedTransactionFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.docero.data.*;

import javax.sql.DataSource;

@Configuration
@Import({DDataConfiguration.class})
@EnableTransactionManagement
@EnableCaching
@EnableDDataConfiguration(packageClass = TestsConfig.class)
public class TestsConfig {
    @Bean
    CacheManager cacheManager() {
        return new ConcurrentMapCacheManager(DData.cacheNames);
    }

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
            DDataResources dDataResources,
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
        bean.setMapperLocations(dDataResources.asArray());
        bean.setTransactionFactory(transactionManager);
        bean.setObjectFactory(DData.getObjectFactory());
        bean.setTypeHandlers(new TypeHandler[]{new UUIDTypeHandler()});
        new MyBatisSpringConfigurationFactory().setApplicationContext(context);

        return bean;
    }

    @DependsOn("dData")
    @Bean
    public RemoteRepository remoteRepository() {
        RemoteRepository remote = new RemoteRepositoryImpl();
        return DData.registerRemote(remote, RemoteBean.class);
    }
}
