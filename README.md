# docero-data

Code Generation Library for MyBatis

**Генерация кода для репозиториев данных MyBatis проекта Доцеро**

Подключаем в Maven pom.xml

    <plugin>
        <artifactId>maven-compiler-plugin</artifactId>
        <configuration>
            <source>1.8</source>
            <target>1.8</target>
            <annotationProcessors>
                <annotationProcessor>org.docero.data.processor.DDataProcessor</annotationProcessor>
            </annotationProcessors>
        </configuration>
    </plugin>

            
Создаём интерфейсы описания сущностей (таблиц БД)

    @DDataBean(value = "sample", table = "sample", schema = "ddata")
    public interface Sample extends Serializable {
        @DDataProperty("i")
        int getInnerId();
        void setInnerId(int val);
        
        @GeneratedValue("ddata.sample_seq")
        @DDataProperty(value="id", id=true)
        int getId();
        void setId(int val);
        
        @DDataProperty(value="s", nullable=false)
        String getStrParameter();
        void setStrParameter(String val);
        
        Inner getInner();
        void setInner(Inner inner);
        
        List<? extends Inner> getListParameter();
        void setListParameter(List<? extends Inner> list);
    }

Запускаем компиляцию. В ходе компиляции в текущем пакете 
создаются аннотации @Sample_Map_ и структура Sample_ с
помощью которых мы продолжаем описание сущности.

    @DDataBean(value = "sample", table = "sample", schema = "ddata")
    public interface Sample extends Serializable {
        ...
        
        @Sample_Map_(value = Sample_.INNER_ID, inner = Inner_.ID)
        Inner getInner();
        void setInner(Inner inner);
        
        @Sample_Map_(listParameter = Inner_.SAMPLE_ID)
        List<? extends Inner> getListParameter();
        void setListParameter(List<? extends Inner> list);
    }

Создаём интерфейс репозитория для доступа к сущности

    @DDataRep
    public interface SampleRepository extends DDataRepository<Sample, Integer> {
        List<Sample> list(
                String val,
                Integer listId, //exists record in list of inner
                String listLike, //exists record in list of inner
                String innerText, //TODO inner text is
                Integer inner, //inner_id is
                DDataOrder<Sample_> sort,
                RowBounds bounds
        ) throws IOException, IllegalArgumentException;
        
        long count();
        
        List<Sample> testForNull(
                boolean innerIsNull);
    }

Запускаем компиляцию. В ходе компиляции в текущем пакете 
создаются аннотации @SampleRepository_DDataFetch_ и 
@SampleRepository_Filter_ с помощью которых мы уточняем
пользовательские методы

    @DDataRep
    public interface SampleRepository extends DDataRepository<Sample, Integer> {
        @SampleRepository_DDataFetch_(value = DDataFetchType.COLLECTIONS_ARE_LAZY, eagerTrunkLevel = 1)
        List<Sample> list(
                @SampleRepository_Filter_(
                        value = Sample_.STR_PARAMETER,
                        option = DDataFilterOption.LIKE
                ) String val,
                @SampleRepository_Filter_(
                        listParameter = Inner_.ID
                ) Integer listId, //exists record in list of inner
                @SampleRepository_Filter_(
                        listParameter = Inner_.TEXT,
                        option = DDataFilterOption.LIKE_STARTS
                ) String listLike, //exists record in list of inner
                @SampleRepository_Filter_(
                        inner = Inner_.TEXT,
                        option = DDataFilterOption.LIKE_STARTS
                ) String innerText, //TODO inner text is
                @SampleRepository_Filter_(
                        Sample_.INNER_ID
                ) Integer inner, //inner_id is
                DDataOrder<Sample_> sort,
                RowBounds bounds
        ) throws IOException, IllegalArgumentException;
        
        @SampleRepository_DDataFetch_(select = "SELECT COUNT(*) FROM \"ddata\".\"sample\"")
        long count();
        
        List<Sample> testForNull(
                @SampleRepository_Filter_(
                        value = Sample_.INNER_ID,
                        option = DDataFilterOption.IS_NULL
                ) boolean innerIsNull);
    }

ну а методы get, update, insert и delete определены интерфейсом
DDataRepository и их пока переопределять не будем. Ну и используем
наш код (ну да Spring, куда без него)

    @Configuration
    @Import({DDataConfiguration.class})
    @EnableTransactionManagement
    @EnableCaching
    public class TestsConfig {
        @Bean
        CacheManager cacheManager() {
            return new ConcurrentMapCacheManager(DData.cacheNames);
        }
        @Bean
        public SqlSessionFactoryBean sqlSessionFactoryBean(
                DataSource dataSource,
                DDataResources dDataResources,
                TransactionFactory transactionManager
        ) throws IOException {
            SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
            bean.setDataSource(dataSource);
            bean.setMapperLocations(dDataResources.asArray());
            bean.setTransactionFactory(transactionManager);
            bean.setObjectFactory(DData.getObjectFactory());
            return bean;
        }
        ...
    }
        
    @Autowired
    private SampleRepository iSampleRepository;
        
    Sample bean = iSampleRepository.get(1);
        
    List<Sample> sl;
    sl = iSampleRepository.list(null, null, null, null, null,
         DDataOrder.asc(Sample_.ID).addDesc(Sample_.INNER_ID),
         new RowBounds(0, 10));

**Интерфейсы**

DDataVersionalBean&lt;T extends Temporal&gt; предназначен для работы
с версионными репозиториями, расширяет Serializable
(все сущности должны поддерживать сериализацию) и 
указывает на тип ключа версии.
 
DDataRepository&lt;T extends Serializable, C extends Serializable&gt; 
репозиторий данных для сущностей 
типа &lt;T&gt; с ключом типа &lt;C&gt;, определяет минимальный набор
методов работы с данными:

    T create();
    T get(C id);
    void insert(T bean);
    void update(T bean);
    void delete(C id);

DDataVersionalRepository&lt;T extends DDataVersionalBean&lt;A&gt;, C extends Serializable, A extends Temporal&gt;
репозиторий версионных данных (таблицы с двумя или более 
ключевых полей, одно из которых значение версии), расширяет
DDataRepository и добавляет метод:
 
    T get(C id, A at);

DDataBatchOpsRepository репозиторий для выполнения 
batch-операций, определяет методы:

    <T extends Serializable> T create(Class<T> clazz);
    <T extends Serializable> T get(Class<T> clazz, Serializable id);
    void insert(Serializable bean);
    void update(Serializable bean);
    List<BatchResult> flushStatements();

**Статические анотации**

@DDataBean определение сущности

    String value() default "" - имя для использования в MyBatis XML mapping
    String schema() default "" - имя схемы в БД
    String table() default "" - имя таблицы в БД
    DictionaryType dictionary() default DictionaryType.NO
        DictionaryType.NO - не справочник, не использовать кэш
        DictionaryType.SMALL - небольшой справочник, максимально используем кеш
        DictionaryType.LARGE - большой справочник, минимально используем кеш

@DDataProperty определение связи свойства с колонкой таблицы

    String value() default "" - имя колонки
    boolean id() default false - является первичным ключом
    boolean versionFrom() default false - первичный ключ, значение версии
    boolean versionTo() default false - используется как колонка 
                                        с верхним ограничение версии
    boolean nullable() default true - может быть пустым
    int length() default 0 - длина CHAR и VARCHAR, 0 - без ограничений
    String reader() default "" - SQL выражение используемое для чтения
        например: st_asgeojson(?)
    String writer() default "" - SQL выражение используемое для записи
        например: st_geomfromgeojson(?)

@GeneratedValue колонка с автогенерацией значения при 
вставке записи

    GenerationType strategy() default GenerationType.SEQUENCE;
        GenerationType.SEQUENCE - в value имя последовательности
        GenerationType.SELECT - в value SQL выражение
    String value() default "";
    boolean before() default true - при false сначала вставляет
                    запись, и только потом выполняет генерацию

@DDataRep определяет интерфейс репозитория данных

    Class[] beans() default {} 
    -   массив репозиториев объединяемых DDataBatchOpsRepository
    DDataDiscriminator[] discriminator() default {}
    -   массив @DDataDiscriminator для таблиц содержащих
        несколько сущностей разделяемых по полю

@DDataDiscriminator разделение сущностей таблицы

    String value() значение поля как текст, а вот какого поля
            определит аннотация @RepositoryInterface_Discriminator_
    Class bean() интерфейс сущности определенной @DDataBean

@SelectId определяет id SQL выражения из MyBatis XML mapping файла
используемое для метода репозитория

**Динамические аннотации**

Динамические аннотации генерируются в пакетах содержащих 
соотвествующие сущности и репозитории. Аннотации генерирутся 
с областью видимости только для классов пакета.

@BeanInterface_Map_ определяет соотношение сущностей, 
где value содержит значение из структуры текущей таблицы, а
наименования "свойств" значения из структур связанных таблиц

@RepositoryInterface_DDataFetch_ определение метода репозитория

    DDataFetchType value() default DDataFetchType.COLLECTIONS_ARE_LAZY
    - связные сущности:
        DDataFetchType.NO - не возвращать 
        DDataFetchType.LAZY - возвращать отдельными запросами
        DDataFetchType.EAGER - в одном запросе, до 
                вложенности eagerTrunkLevel
        DDataFetchType.COLLECTIONS_ARE_LAZY - возвращать
                отдельными запросами списочные свойства,
                остальные до вложенности eagerTrunkLevel
    String select() default "" - SQL выражение для выполнения метода
            может содержать имена параметров как :parameterName
    String resultMap() default "" - при использовании select
            можно указать id ResultMap из MyBatis XML mapping файла
    BeanInterface_[] ignore() default NONE_ - поля исключаемые
            из загрузки данным методом, они просто будут null
    int eagerTrunkLevel() default 1 - уровень вложенности связных
            сущностей после которого EAGER-загрузка не производится
        0 - всё возращаем отдельными запросами
        N - в одном запросе связные сущности до уровня N, а 
            останые не возвращаем вообще или отдельными запросами
    boolean truncateLazy() default false 
        - не возвращать связные сущности с вложенностью более
        eagerTrunkLevel 
    BeanInterface_[] defaultOrder() default {} - сортировка

@RepositoryInterface_Filter_ определение параметра метода,
задающего фильтр для возвращаемых значений, могут быть
фильтры по свойствам объектов первого уровня вложенности

    BeanInterface_ value() default NONE_ 
    - колонка таблицы, или
    AnotherBeanInterface_ propertyName() default NONE_ 
    - колонка связной таблицы
        
    DDataFilterOption option() default DDataFilterOption.EQUALS

@RepositoryInterface_Discriminator_ определение поля 
сущности для которой определён репозиторий

...

как говорится to be continued...