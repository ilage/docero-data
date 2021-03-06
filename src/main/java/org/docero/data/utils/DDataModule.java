package org.docero.data.utils;

import org.apache.ibatis.session.SqlSessionFactory;
import org.docero.data.DData;
import org.docero.data.DDataRepository;

import java.io.Serializable;
import java.util.Map;

public interface DDataModule {
    <T extends Serializable, C extends Serializable> java.util.Map<Class<T>, Class<C>>
    getImplementations();

    String[] getCacheNames();

    Map<Class<?>, com.fasterxml.jackson.databind.JsonDeserializer<?>> getDeserializers();

    String[] resources();

    <T extends Serializable, C extends Serializable, R extends DDataRepository<T, C>>
    Class<R> getBeanRepositoryInterface(Class<T> beanClass);

    <T extends Serializable, C extends Serializable> DDataRepository<T, C> getBeanRepository(Class<T> beanClass, SqlSessionFactory sessionFactory);

    <R> R getRepositoryByInterface(Class<R> clazz, SqlSessionFactory sessionFactory);

    void register(org.docero.data.DData dData, org.docero.data.DDataDictionariesService ds);

    Map<Class,Class> getRepositories();

    <T extends java.io.Serializable, C extends java.io.Serializable> T save(
            T t,
            Class beanInterface,
            SqlSessionFactory sqlSessionFactory,
            UpdateOptions updateOptions,
            DData dData
    );
}
