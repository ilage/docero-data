package org.docero.data.tests;

import org.apache.ibatis.transaction.TransactionFactory;
import org.docero.data.utils.DDataDictionariesService;
import org.docero.data.utils.DDataObjectFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.transaction.SpringManagedTransactionFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.docero.data.*;

import javax.sql.DataSource;
import java.io.IOException;

@Configuration
@Import({DDataConfiguration.class})
@EnableTransactionManagement
@EnableCaching
public class TestsConfig {
    @Bean
    CacheManager cacheManager() {
        return new ConcurrentMapCacheManager(
                new StringBuilder(org.docero.data.example.Inner.class.getCanonicalName()).reverse().toString()
        );
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
            DDataDictionariesService dictionariesService,
            TransactionFactory transactionManager
    ) throws IOException {
        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
        bean.setDataSource(dataSource);
        bean.setMapperLocations(dDataResources.asArray());
        bean.setTransactionFactory(transactionManager);
        bean.setObjectFactory(new DDataObjectFactory(dictionariesService));
        return bean;
    }
}
