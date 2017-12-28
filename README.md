# docero-data

Code Generation Library for MyBatis

**Генерация кода для репозиториев данных MyBatis проекта Доцеро**

Подключаем в Maven pom.xml

            <plugin>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
                <executions>
                    <execution>
                        <id>default-testCompile</id>
                        <phase>generate-test-sources</phase>
                        <goals>
                            <goal>testCompile</goal>
                        </goals>
                        <configuration>
                            <annotationProcessors>
                                <annotationProcessor>org.docero.data.processor.DDataProcessor</annotationProcessor>
                            </annotationProcessors>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
            
Создаём интерфейсы описания сущностей (таблиц БД)

    @DDataBean(value = "sample", table = "sample", schema = "ddata")
    public interface Sample extends SampleTable {
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
    public interface Sample extends SampleTable {
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
                DDataOrder<Sample_> sort
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
                @SampleRepository_Filter_(option = DDataFilterOption.START) int start,
                @SampleRepository_Filter_(option = DDataFilterOption.LIMIT) int limit,
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
                DDataOrder<Sample_> sort
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
        ...
    }
        
    @Autowired
    private SampleRepository iSampleRepository;
        
    Sample bean = iSampleRepository.get(1);
        
    List<Sample> sl;
    sl = iSampleRepository.list(0, 10, null, null, null, null, null,
         DDataOrder.asc(Sample_.ID).addDesc(Sample_.INNER_ID));

как говорится to be continued...