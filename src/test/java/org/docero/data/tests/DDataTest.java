package org.docero.data.tests;

import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSessionFactory;
import org.docero.data.*;
import org.docero.data.beans.*;
import org.docero.data.repositories.*;
import org.docero.data.rmt.Remote_WB_;
import org.docero.data.utils.*;
import org.docero.data.view.*;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.Commit;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import javax.xml.bind.*;

import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {
        TestsConfig.class,
        DDataConfiguration.class
})
@ActiveProfiles("test")
@SuppressWarnings("SpringJavaAutowiringInspection")
public class DDataTest {
    private static final Logger LOG = LoggerFactory.getLogger(DDataTest.class);
    @Autowired
    private ApplicationContext springContext;
    @Autowired
    private DData dData;
    @Autowired
    private DataSource dataSource;
    @Autowired
    private SqlSessionFactory sqlSessionFactory;
    @Autowired
    private SampleRepository iSampleRepository;
    @Autowired
    private DDataRepository<Inner, Integer> iInnerRepository;
    @Autowired
    private CompositeKeyRepository iCKSampleRepository;
    @Autowired
    private VersionalSampleRepository iVSample;//DDataVersionalRepository<HistSample, Integer, LocalDateTime> iVSample;
    @Autowired
    private DDataVersionalRepository<HistInner, Integer, LocalDateTime> iVInner;
    @Autowired
    private SampleBatchOps sampleBatchOps;
    @Autowired
    private MultiTypesRepository multiTypesRepository;
    @Autowired
    private DDataViewBuilder viewBuilder;

    @SuppressWarnings("SqlNoDataSourceInspection")
    public void setUp() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement("" +
                    "DROP TABLE IF EXISTS ddata.\"inner\";" +
                    "DROP TABLE IF EXISTS ddata.\"sample\";" +
                    "CREATE TABLE ddata.\"sample\" (\n" +
                    "  id INT NOT NULL,\n" +
                    "  s VARCHAR(125),\n" +
                    "  i INT,\n" +
                    "  remote_id INT,\n" +
                    "  hash BYTEA,\n" +
                    "  uuid UUID, \n" +
                    "  CONSTRAINT \"sample_pkey\" PRIMARY KEY (id)\n" +
                    ");\n" +
                    "\n" +
                    "CREATE TABLE ddata.\"inner\" (\n" +
                    "  id INT NOT NULL,\n" +
                    "  text VARCHAR(125),\n" +
                    "  sample_id INT,\n" +
                    "  d1 INT,\n" +
                    "  CONSTRAINT \"inner_pkey\" PRIMARY KEY (id)\n" +
                    ");" +
                    "ALTER TABLE ddata.\"inner\"\n" +
                    "  ADD CONSTRAINT inner_sample_fk FOREIGN KEY (sample_id) REFERENCES ddata.sample (id)\n" +
                    "   ON UPDATE CASCADE ON DELETE CASCADE\n" +
                    "   DEFERRABLE;" +
                    "INSERT INTO ddata.\"sample\" (id, s, i, remote_id) VALUES (1,'s1',1001,2);\n" +
                    "INSERT INTO ddata.\"sample\" (id, s, i, remote_id) VALUES (2,'s2',1003,1);\n" +
                    "\n" +
                    "DROP FUNCTION IF EXISTS ddata.sample_proc(INTEGER);\n" +
                    "\n" +
                    "CREATE OR REPLACE FUNCTION ddata.sample_proc(rid INTEGER)\n" +
                    "  RETURNS TABLE (id INT, s VARCHAR, t2_text VARCHAR, t2_id INT, t2_sample_id INT) AS\n" +
                    "$BODY$\n" +
                    "SELECT\n" +
                    "  t0.\"id\" AS \"id\",\n" +
                    "  t0.\"s\" AS \"s\",\n" +
                    "  t2.\"text\" AS t2_text,\n" +
                    "  t2.\"id\" AS t2_id,\n" +
                    "  t2.\"sample_id\" AS t2_sample_id\n" +
                    "FROM \"ddata\".\"sample\" AS t0\n" +
                    "LEFT JOIN \"ddata\".\"inner\" AS t2 ON (t0.\"id\" = t2.\"sample_id\")\n" +
                    "$BODY$\n" +
                    "  LANGUAGE SQL;\n" +
                    "\n" +
                    "INSERT INTO ddata.\"inner\" (id, text, sample_id) VALUES (1001,'i1',1);\n" +
                    "INSERT INTO ddata.\"inner\" (id, text, sample_id) VALUES (1002,'i2',1);\n" +
                    "INSERT INTO ddata.\"inner\" (id, text, sample_id) VALUES (1003,'i3',2);" +
                    "" +
                    "DROP TABLE IF EXISTS ddata.\"h1\";" +
                    "CREATE TABLE ddata.\"h1\" (\n" +
                    "  id INT NOT NULL,\n" +
                    "  date_from TIMESTAMP NOT NULL,\n" +
                    "  date_to TIMESTAMP,\n" +
                    "  s VARCHAR(125),\n" +
                    "  s2 NUMERIC,\n" +
                    "  \"inner\" INT,\n" +
                    "  CONSTRAINT \"sample_h1\" PRIMARY KEY (id,date_from)\n" +
                    ");\n" +
                    "INSERT INTO ddata.\"h1\" (id, date_from, date_to, s, \"inner\") VALUES (1,'2017-01-01T00:00:00','2017-01-02T00:00:00','h1',10);\n" +
                    "INSERT INTO ddata.\"h1\" (id, date_from, s, \"inner\") VALUES (1,'2017-01-02T00:00:00','h1v1',10);\n" +
                    "INSERT INTO ddata.\"h1\" (id, date_from, s, \"inner\") VALUES (2,'2017-01-02T00:00:00','h2',11);\n" +
                    "DROP TABLE IF EXISTS ddata.\"h2\";" +
                    "CREATE TABLE ddata.\"h2\" (\n" +
                    "  id INT NOT NULL,\n" +
                    "  date_from TIMESTAMP NOT NULL,\n" +
                    "  date_to TIMESTAMP,\n" +
                    "  s VARCHAR(125),\n" +
                    "  CONSTRAINT \"sample_h2\" PRIMARY KEY (id,date_from)\n" +
                    ");\n" +
                    "INSERT INTO ddata.\"h2\" (id, date_from, s) VALUES (10,'2017-01-01T00:00:00','hi1');\n" +
                    "INSERT INTO ddata.\"h2\" (id, date_from, date_to, s) VALUES (11,'2017-01-02T00:00:00','2017-01-03T00:00:00','hi2');\n" +
                    "INSERT INTO ddata.\"h2\" (id, date_from, s) VALUES (11,'2017-01-03T00:00:00','hi2v1');\n" +
                    "" +
                    "DROP TABLE IF EXISTS ddata.\"a1\";" +
                    "CREATE TABLE ddata.a1 (\n" +
                    "  id INT NOT NULL,\n" +
                    "  elem_type INT NOT NULL DEFAULT 0,\n" +
                    "  linked INT,\n" +
                    "  sm INT,\n" +
                    "  lg INT,\n" +
                    "  CONSTRAINT a1_pk PRIMARY KEY (id)\n" +
                    ");" +
                    "INSERT INTO ddata.a1 (id,elem_type) VALUES (1,1);\n" +
                    "INSERT INTO ddata.a1 (id,elem_type,linked,sm,lg) VALUES (2,1,1001,1,1);\n" +
                    "INSERT INTO ddata.a1 (id,elem_type,linked,sm,lg) VALUES (3,2,1,2,2);\n" +
                    "INSERT INTO ddata.a1 (id,elem_type,linked,sm,lg) VALUES (4,3,3,2,2);\n" +
                    "" +
                    "DROP TABLE IF EXISTS ddata.\"smdict\";" +
                    "CREATE TABLE ddata.\"smdict\" (\n" +
                    "  id INT NOT NULL," +
                    "  parent_id INT," +
                    "  name VARCHAR,\n" +
                    "  CONSTRAINT smdict_pk PRIMARY KEY (id)\n" +
                    ");" +
                    "INSERT INTO ddata.\"smdict\" (id,parent_id,name) VALUES (1,NULL,'КС знач 1');" +
                    "INSERT INTO ddata.\"smdict\" (id,parent_id,name) VALUES (2,1,'КС знач 2');" +
                    "" +
                    "DROP TABLE IF EXISTS ddata.\"lgdict\";" +
                    "CREATE TABLE ddata.\"lgdict\" (\n" +
                    "  id INT NOT NULL," +
                    "  name VARCHAR,\n" +
                    "  CONSTRAINT lgdict_pk PRIMARY KEY (id)\n" +
                    ");" +
                    "INSERT INTO ddata.\"lgdict\" (id,name) VALUES (1,'БС знач 1');" +
                    "INSERT INTO ddata.\"lgdict\" (id,name) VALUES (2,'БС знач 2');" +
                    "" +
                    "DROP TABLE IF EXISTS ddata.\"smgraf\";\n" +
                    "CREATE TABLE ddata.\"smgraf\" (\n" +
                    "  parent INT NOT NULL," +
                    "  child INT NOT NULL," +
                    "  CONSTRAINT smgraf_pk PRIMARY KEY (parent,child)" +
                    ");" +
                    "INSERT INTO ddata.\"smgraf\" (parent,child) VALUES (1,1);" +
                    "INSERT INTO ddata.\"smgraf\" (parent,child) VALUES (1,2);" +
                    "" +
                    "DROP SEQUENCE IF EXISTS ddata.sample_seq;\n" +
                    "\n" +
                    "CREATE SEQUENCE ddata.sample_seq\n" +
                    "  INCREMENT 1\n" +
                    "  MINVALUE 1\n" +
                    "  MAXVALUE 9223372036854775807\n" +
                    "  START 2000\n" +
                    "  CACHE 1;")) {
                st.execute();
            }
        }
    }

    @Test
    public void sqlTest() {
        DSQL sql = new DSQL();
        DSQL whereGroup = new DSQL();
        whereGroup.WHERE("c1")
                .OR().WHERE("c2")
                .OR().WHERE("c3");
        sql.SELECT("*").FROM("t")
                .WHERE(whereGroup).AND().WHERE("c4");
        assertEquals("SELECT *\nFROM t\nWHERE (( (c1) \nOR (c2) \nOR (c3))) \nAND (c4)", sql.toString());
    }

    @Test
    @Transactional
    public void viewTest() throws SQLException, DDataException {
        setUp();

        DDataView view = viewBuilder.build(Sample_WB_.class, new ArrayList<DDataFilter>() {{
            add(new DDataFilter(Sample_WB_.ID));
            add(new DDataFilter(Sample_WB_.STR_PARAMETER));
            DDataFilter iCols = new DDataFilter(Sample_WB_.INNER);
            iCols.add(new DDataFilter(Inner_WB_.TEXT));
            add(new DDataFilter(Sample_WB_.REMOTE_BEAN) {{
                add(new DDataFilter(Remote_WB_.NAME));
            }});
            add(iCols);
        }});
        view.setFilter(new DDataFilter() {{
            add(new DDataFilter(Sample_WB_.ID, DDataFilterOperator.GREATE, 0));

            add(new DDataFilter(Sample_WB_.INNER) {{
                add(new DDataFilter(Inner_WB_.ID, DDataFilterOperator.GREATE, 0));
            }});

            add(new DDataFilter(Sample_WB_.LIST_PARAMETER) {{
                add(new DDataFilter(Inner_WB_.ID, DDataFilterOperator.GREATE, 0));
                add(new DDataFilter(Inner_WB_.ID, DDataFilterOperator.LESS, 100000));
            }});
        }});
        DDataViewRows rows = view.select(0, 100);
        assertEquals(2, rows.size());
        assertEquals(2, view.count());
        DDataViewRow row = rows.getRow(0);
        assertNotNull(row);
        String n = (String) row.getColumnValue(0, Sample_WB_.REMOTE_BEAN, Remote_WB_.NAME);
        assertNotNull(n);

        //DataViewBuilder viewBuilder = new DataViewBuilder(sqlSessionFactory);
        view = viewBuilder.build(new ArrayList<Class<? extends DDataAttribute>>() {{
            add(ItemSample_WB_.class);
            add(ItemInner_WB_.class);
            add(ItemItemSample_WB_.class);
        }}, new ArrayList<DDataFilter>() {{
            add(new DDataFilter(ItemSample_WB_.ID) {{
                setSortAscending(false);
            }});
            add(new DDataFilter(ItemSample_WB_.ELEM_TYPE));
            add(new DDataFilter(ItemSample_WB_.SM_ID));

            add(new DDataFilter(ItemItemSample_WB_.SAMPLE) {{
                setMapName("iisample");
                add(new DDataFilter(ItemSample_WB_.SAMPLE) {{
                    add(new DDataFilter(Sample_WB_.INNER) {{
                        this.add(new DDataFilter(Inner_WB_.TEXT));
                    }});
                }});
            }});

            add(new DDataFilter(ItemSample_WB_.SAMPLE) {{
                add(new DDataFilter(Sample_WB_.STR_PARAMETER));
            }});
            add(new DDataFilter(ItemSample_WB_.SAMPLE) {{
                add(new DDataFilter(Sample_WB_.LIST_PARAMETER) {{
                    add(new DDataFilter(Inner_WB_.TEXT));
                    add(new DDataFilter(Inner_WB_.ID));
                    // checked if present add(new DDataFilter(Inner_WB_.SAMPLE_ID));
                    add(new DDataFilter(Inner_WB_.ID, DDataFilterOperator.LESS, 5555));

                    this.add(new DDataFilter(Inner_WB_.V1) {{
                        this.add(new DDataFilter(SmallDict_WB_.NAME));
                    }});

                    this.add(new DDataFilter(Inner_WB_.SAMPLE) {{
                        this.add(new DDataFilter(Sample_WB_.HASH));
                    }});
                }});
            }});
            add(new DDataFilter(ItemSample_WB_.SAMPLE) {{
                add(new DDataFilter(Sample_WB_.HASH));
            }});
            add(new DDataFilter(ItemSample_WB_.SAMPLE) {{
                add(new DDataFilter(Sample_WB_.INNER) {{
                    this.add(new DDataFilter(Inner_WB_.TEXT));
                }});
            }});

            add(new DDataFilter(ItemInner_WB_.INNER) {{
                add(new DDataFilter(Inner_WB_.TEXT));
            }});
        }});
        view.setFilter(new DDataFilter() {{
            add(new DDataFilter(ItemSample_WB_.ID, DDataFilterOperator.GREATE, 0));

            add(new DDataFilter(ItemInner_WB_.INNER) {{
                add(new DDataFilter(Inner_WB_.ID, DDataFilterOperator.GREATE, 0));
                add(new DDataFilter(Inner_WB_.ID, DDataFilterOperator.LESS, 100000));
            }});
        }});
        DDataViewRows viewResult = view.select(0, 100);
        assertEquals(3, viewResult.size());
        Map<Object, Object> map;
        assertNotNull(row = viewResult.getRow((Object) 3));
        Object[] v = row.getColumn(ItemSample_WB_.SAMPLE, Sample_WB_.LIST_PARAMETER, Inner_WB_.TEXT);
        assertEquals(2, v.length);

        /*view.addUpdateService(Inner.class, new DDataBeanUpdateService<Inner>(Inner.class) {
            @Override
            protected Inner createBean() {
                return iInnerRepository.create();
            }

            @Override
            protected Inner updateBean(Inner bean) {
                return bean;
            }

            @Override
            public boolean serviceDoesNotMakeUpdates() {
                return true;
            }
        });*/

        row.setColumnValue("text value", 0,
                ItemSample_WB_.SAMPLE, Sample_WB_.STR_PARAMETER);

        byte[] testHash = new byte[]{0, 1, 0, 1};
        row.setColumnValue(testHash, 0,
                ItemSample_WB_.SAMPLE, Sample_WB_.HASH);

        row.setColumnValue("updated text value", 1,
                ItemSample_WB_.SAMPLE, Sample_WB_.LIST_PARAMETER, Inner_WB_.TEXT);

        assertEquals("updated text value", row.getColumnValue(1,
                ItemSample_WB_.SAMPLE, Sample_WB_.LIST_PARAMETER, Inner_WB_.TEXT));

        row.setColumnValue("test for insert", 2,
                ItemSample_WB_.SAMPLE, Sample_WB_.LIST_PARAMETER, Inner_WB_.TEXT);

        row.setColumnValue("update linked", 0,
                ItemSample_WB_.SAMPLE, Sample_WB_.INNER, Inner_WB_.TEXT);

        Integer sample_id = (Integer) row.getColumnValue(0, ItemSample_WB_.ID);

        Integer inner_id = (Integer) row.getColumnValue(1,
                ItemSample_WB_.SAMPLE, Sample_WB_.LIST_PARAMETER, Inner_WB_.ID);

        row.setColumnValue("insert linked", 2,
                ItemSample_WB_.SAMPLE, Sample_WB_.LIST_PARAMETER, Inner_WB_.V1, SmallDict_WB_.NAME);

        row.setColumnValue("double insert linked", 3,
                ItemSample_WB_.SAMPLE, Sample_WB_.LIST_PARAMETER, Inner_WB_.V1, SmallDict_WB_.NAME);

        view.flushUpdates(t -> {
            t.printStackTrace();
            throw new RuntimeException("Too many errors", t);
        });

        assertEquals(viewResult.size(), viewResult.toList().size());

        List<Map<String, Object>> sl = viewResult.toListStrict();
        assertEquals(viewResult.size(), sl.size());
        assertTrue(sl.stream().allMatch(e -> e.get("dDataBeanKey_") != null));

        assertEquals("updated text value", iInnerRepository.get(inner_id).getText());

        ItemSample sample_o = multiTypesRepository.get(sample_id);
        assertNotNull(sample_o);
        assertNotNull(sample_o.getSample());
        assertEquals("text value", sample_o.getSample().getStrParameter());
        assertArrayEquals(testHash, sample_o.getSample().getHash());
        assertNotNull(sample_o.getSample().getInner());
        assertEquals("update linked", sample_o.getSample().getInner().getText());
        assertNotNull(sample_o.getSample().getListParameter());
        assertTrue(
                sample_o.getSample().getListParameter().stream()
                        .anyMatch(i -> "test for insert".equals(i.getText()))
        );

        assertEquals(3, view.count());

        view = viewBuilder.build(Sample_WB_.class, new ArrayList<DDataFilter>() {{
            add(new DDataFilter(Sample_WB_.LIST_PARAMETER, DDataFilterOperator.COUNT));
        }});
        int[] maxCount = view.aggregateInt(DDataFilterOperator.MAX);
        assertNotNull(maxCount);
        assertEquals(1, maxCount.length);
        assertEquals(4, maxCount[0]);

        view = viewBuilder.build(Sample_WB_.class, new ArrayList<DDataFilter>() {{
            add(new DDataFilter(Sample_WB_.LIST_PARAMETER) {{
                add(new DDataFilter(Inner_WB_.TEXT));
                add(new DDataFilter(Inner_WB_.SAMPLE_ID));
            }});
        }});
        view.setFilter(new DDataFilter() {{
            add(new DDataFilter(Sample_WB_.ID, DDataFilterOperator.GREATE, 0));

            add(new DDataFilter(Sample_WB_.LIST_PARAMETER) {{
                add(new DDataFilter(Inner_WB_.ID, DDataFilterOperator.GREATE, 0));
                add(new DDataFilter(Inner_WB_.ID, DDataFilterOperator.LESS, 100000));
            }});
        }});

        row = view.select(0, 100).getRow(0);
        assertEquals("update linked", row.getColumnValue(0, "listParameter.text"));
        assertEquals("updated text value", row.getColumnValue(1, "listParameter.text"));
        row.setColumnValue("Test1", 0, "listParameter.text");
        assertEquals("Test1", row.getColumnValue(0, "listParameter.text"));
        view.flushUpdates(t -> {
            t.printStackTrace();
            throw new RuntimeException("Too many errors", t);
        });
        rows = view.select(0, 100);
        assertEquals("Test1", rows.getRow(0).getColumnValue(0, "listParameter.text"));
    }

    @Test
    @Transactional
    public void versionalViewTest() throws Exception {
        setUp();

        LocalDateTime beforeUpdate = LocalDateTime.now().minusSeconds(1);

        DDataView view = viewBuilder.build(HistSample_WB_.class, new ArrayList<DDataFilter>() {{
            add(new DDataFilter(HistSample_WB_.VALUE));
            add(new DDataFilter(HistSample_WB_.NUMERIC));
            add(new DDataFilter(HistSample_WB_.INNER) {{
                setMapName("inner007");
                add(new DDataFilter(HistInner_WB_.TEXT) {{
                    setMapName("txt");
                }});
                add(new DDataFilter(HistInner_WB_.VERSION_FROM) {{
                    setMapName("version");
                }});
            }});
        }});
        view.setFilter(new DDataFilter() {{
            add(new DDataFilter(HistSample_WB_.ID, DDataFilterOperator.GREATE, 0));
        }});

        DDataViewRows vr = view.select(0, 100);
        DDataViewRow row = vr.getRow(0);
        Integer t_sample_id = (Integer) row.getColumnValue(0, HistSample_WB_.ID);

        LocalDateTime time = LocalDateTime.now();
        row.setColumnValue("update sample", 0, HistSample_WB_.VALUE);
        row.setColumnValue(101, 0, HistSample_WB_.NUMERIC);
        row.setColumnValue("update inner", 0, "inner007.txt");
        row.setColumnValue(time, 0, "inner007.version");

        view.flushUpdates(t -> {
            t.printStackTrace();
            throw new RuntimeException("Too many errors", t);
        });

        assertEquals("update sample", row.getColumnValue(0, HistSample_WB_.VALUE));
        assertEquals(101, row.getColumnValue(0, HistSample_WB_.NUMERIC));
        assertEquals("update inner", row.getColumnValue(0, "inner007.txt"));
        assertEquals(time, row.getColumnValue(0, "inner007.version")); // this value being ignored in update

        HistSample bean = iVSample.get(t_sample_id);
        assertNotNull(bean);
        assertNotNull(bean.getInner());
        assertEquals("update sample", bean.getValue());
        assertEquals(Integer.valueOf(101), bean.getNumeric());
        assertEquals("update inner", bean.getInner().getText());
        assertNotEquals(time, bean.getInner().getDateFrom()); // update version was ignored

        bean = iVSample.get(t_sample_id, beforeUpdate);
        assertNotNull(bean);
        assertNotNull(bean.getInner());
        assertNotEquals("update inner", bean.getInner().getText());
        assertNotEquals(time, bean.getInner().getDateFrom()); // update version was ignored
    }


    @Test
    @Transactional
    @Commit
    public void multiTypesTest() throws SQLException, IOException, DDataException {
        setUp();

        assertNotNull(multiTypesRepository);
        ItemAbstraction s = multiTypesRepository.get(3);
        assertNotNull(s);
        assertTrue(s instanceof ItemSample);
        assertNotNull(((ItemSample) s).getSample());
        assertNotNull(((ItemSample) s).getSmall());
        assertNotNull(((ItemSample) s).getLarge());

        ItemAbstraction i = multiTypesRepository.get(2);
        assertNotNull(i);
        assertTrue(i instanceof ItemInner);
        assertNotNull(((ItemInner) i).getInner());
        assertNotNull(((ItemInner) i).getLarge());
        assertNotNull(((ItemInner) i).getLargeCached());
        assertEquals(((ItemInner) i).getLarge().getId(), ((ItemInner) i).getLargeCached().getId());

        ((ItemInner) i).setLarge(((ItemSample) s).getLarge());
        multiTypesRepository.update(i);
        assertEquals(((ItemInner) i).getLgId(), ((ItemSample) s).getLgId());
        assertEquals(((ItemInner) multiTypesRepository.get(2)).getLgId(), ((ItemSample) s).getLgId());

        List<ItemAbstraction> l = multiTypesRepository.list(new RowBounds(0, 100));
        assertNotNull(l);
        for (ItemAbstraction a : l)
            if (a.getId() == s.getId()) assertTrue(a instanceof ItemSample);
            else if (a.getId() == i.getId()) assertTrue(a instanceof ItemInner);

        ItemItemSample iis = multiTypesRepository.get(4);
        assertNotNull(iis.getSample());
        assertEquals(3, iis.getSample().getId());
        assertNotNull(iis.getSample().getSample());
    }

    @Test
    @Transactional
    @Commit
    public void batchTest() throws SQLException, IOException {
        setUp();

        assertNotNull(sampleBatchOps);
        Sample s1 = sampleBatchOps.create(Sample.class);
        s1.setStrParameter("batch1");
        sampleBatchOps.insert(s1);
        int s1id = s1.getId();

        Sample s2 = sampleBatchOps.create(Sample.class);
        s2.setStrParameter("batch2");
        sampleBatchOps.insert(s2);
        assertTrue(s1id < s2.getId());

        sampleBatchOps.flushStatements();
        assertEquals(s1id, s1.getId());

        Sample s3 = sampleBatchOps.getOneBy("s%", 1001);
        assertNotNull(s3);
    }

    @Test
    @Transactional
    @Commit
    public void versionalTest() throws SQLException, IOException {
        setUp();

        assertNotNull(iVSample);
        assertNotNull(iVInner);

        HistSample bean = iVSample.get(1);
        assertNotNull(bean);
        int count1 = iVSample.list(1).size();

        //System.out.println(bean.getDateFrom());
        bean.setValue("v1");
        iVSample.update(bean);

        bean = iVSample.get(1);
        assertEquals("v1", bean.getValue());

        bean = iVSample.get(1, LocalDateTime.of(2017, 1, 1, 0, 0, 0));
        assertEquals("h1", bean.getValue());

        LocalDateTime version = LocalDateTime.now();
        bean = iVSample.get(1, version);
        assertEquals(version, bean.getDDataBeanActualAt_());

        List<HistSample> l = iVSample.list(1);
        assertEquals(count1 + 1, l.size());

        LocalDateTime dateModifyInnerRecord = LocalDateTime.of(2017, 1, 3, 0, 0, 0);
        bean = iVSample.get(2, dateModifyInnerRecord.minusDays(1));
        assertNotNull(bean);
        assertNotNull(bean.getInner());
        assertNotEquals(bean.getInner().getDateFrom(), dateModifyInnerRecord);

        l = iVSample.listAt(2, dateModifyInnerRecord.minusDays(1));
        assertNotNull(l);
        assertEquals(1, l.size());
        assertNotEquals(l.get(0).getInner().getDateFrom(), dateModifyInnerRecord);

        bean = iVSample.get(2, dateModifyInnerRecord);
        assertNotNull(bean);
        assertNotNull(bean.getInner());
        assertEquals(bean.getInner().getDateFrom(), dateModifyInnerRecord);

        l = iVSample.listAt(2, dateModifyInnerRecord);
        assertNotNull(l);
        assertEquals(1, l.size());
        assertEquals(l.get(0).getInner().getDateFrom(), dateModifyInnerRecord);
    }


    @Test
    @Transactional
    @Commit
    public void compositeKeyTest() throws SQLException, IOException {
        setUp();

        assertNotNull(iCKSampleRepository);
        CompositeKeySample h = iCKSampleRepository.get(new CompositeKeySample_Key_(
                LocalDateTime.of(2017, 1, 1, 0, 0, 0), 1)
        );
        assertNotNull(h);
        //System.out.println(h.getValue());

        h.setValue("hh1");
        iCKSampleRepository.update(h);

        CompositeKeySample hh = iCKSampleRepository.get(new CompositeKeySample_Key_(
                LocalDateTime.of(2017, 1, 1, 0, 0, 0), 1)
        );
        assertNotNull(hh);
        //System.out.println(hh.getValue());

        assertNotNull(hh.getInner());
        //System.out.println(hh.getInner().getValue());

        List<CompositeKeySample> l = iCKSampleRepository.list(null, "hi1");
        assertEquals(1, l.size());
    }

    @Test
    @Transactional
    @Commit
    public void sampleTest() throws SQLException, IOException {
        setUp();

        assertNotNull(iSampleRepository);
        assertNotNull(iInnerRepository);

        Sample bean = iSampleRepository.get(1);
        assertNotNull(bean);
        assertEquals(1, bean.getId());
        assertNotNull(bean.getRemoteBean());
        assertEquals(2, bean.getRemoteBean().getRemoteId());
        Inner ir = bean.getInner();
        assertNotNull(ir);
        assertEquals(1001, ir.getId());
        List<? extends Inner> lp = bean.getListParameter();
        assertNotNull(lp);
        assertEquals(2, lp.size());

        bean = iSampleRepository.get(2);
        assertNotNull(bean);
        assertEquals(2, bean.getId());
        ir = bean.getInner();
        assertNotNull(ir);
        assertEquals(1003, ir.getId());
        lp = bean.getListParameter();
        assertNotNull(lp);
        assertEquals(1, lp.size());

        List<Sample> sl;
        RowCounter addCount = new RowCounter();
        sl = iSampleRepository.list(null, null, null, null, null,
                addCount,
                DDataOrder.asc(Sample_.ID).addDesc(Sample_.INNER_ID), new RowBounds(0, 10));
        assertEquals(2, sl.size());
        assertEquals(2, addCount.getCount());
        // for buunds== null
        sl = iSampleRepository.list(null, null, null, null, null,
                addCount,
                DDataOrder.asc(Sample_.ID).addDesc(Sample_.INNER_ID), null);
        assertEquals(null, sl);
        assertEquals(2, addCount.getCount());

        sl = iSampleRepository.list(null, null, null, null, 1002, new RowBounds(0, 10));
        assertEquals(0, sl.size());

        sl = iSampleRepository.list(null, 1001, null, null, null, null);
        assertEquals(1, sl.size());

        sl = iSampleRepository.list(null, null, null, "i1", null, new RowBounds(0, 10));
        assertEquals(1, sl.size());

        sl = iSampleRepository.list("s1", null, null, null, null, new RowBounds(0, 10));
        assertEquals(1, sl.size());

        sl = iSampleRepository.list(null, null, "i2", null, null, new RowBounds(0, 10));
        assertEquals(1, sl.size());

        sl = iSampleRepository.list("s2", null, "i2", null, null, new RowBounds(0, 10));
        assertEquals(0, sl.size());


        Sample sample = iSampleRepository.create();
        sample.setStrParameter("a");
        sample.setId(3);
        sample.setInner(iInnerRepository.get(1002));
        sample.setHash(new byte[]{0, 1, 0, 1, 0});
        sample.setUuid(UUID.randomUUID());
        iSampleRepository.insert(sample);

        sl = iSampleRepository.list(null, null, null, null, 1002, null);
        assertEquals(1, sl.size());

        sl = iSampleRepository.listByUUid(sample.getUuid());
        assertEquals(1, sl.size());

        sample = iSampleRepository.get(sample.getId());
        assertEquals("a", sample.getStrParameter());

        iSampleRepository.delete(sample.getId());
        assertNull(iSampleRepository.get(sample.getId()));

        assertEquals(1, iSampleRepository.listForInners(new HashSet<Integer>() {{
            this.add(1001);
            this.add(1002);
        }}).size());
        assertEquals(2, iSampleRepository.listForInners(new HashSet<Integer>() {{
            this.add(1001);
            this.add(1003);
        }}).size());

        iSampleRepository.remove(sample.getUuid(), 1);

        iSampleRepository.removeWithJoins(sample.getUuid(), sample.getInner().getText());
    }

    @Test
    @Transactional
    @Commit
    public void isNullTest() throws SQLException, IOException {
        setUp();

        List<Sample> sl = iSampleRepository.testForNull(false);
        assertEquals(2, sl.size());

        sl = iSampleRepository.testForNull(true);
        assertEquals(0, sl.size());

        Sample s = iSampleRepository.getOneBy("s%", 1001);
        assertNotNull(s);
        assertEquals(1, s.getId());
    }

    @Test
    @Transactional
    public void rowCounterTest() throws SQLException, IOException {
        setUp();
        RowCounter counter = new RowCounter();

        List<Sample> sl = iSampleRepository.list(null, null, null, null, null, counter, null, new RowBounds(0, 1));
        assertEquals(counter.getCount(), sl.size() + 1);

        counter = new RowCounter();
        sl = iSampleRepository.list(null, null, null, null, null, counter, null, new RowBounds(1, 1));
        assertEquals(counter.getCount(), sl.size() + 1);
    }

    @Test
    @Transactional
    // sql-operator SIMILAR TO
    public void similarToTest() throws SQLException, IOException, DDataException {
        setUp();
        /*List<String> values = new ArrayList<String>(){{
            this.add("%1");
            this.add("%2");
        }};*/
        List<String> val = Arrays.asList("%5';", "%2");
        List<Sample> st = iSampleRepository.listSimilarTo(val);
        assertNotNull(st);
        assertEquals(1, st.size());
        // assertEquals(2,st.size());

        DDataView view = viewBuilder.build(Sample_WB_.class, new DDataFilter(Sample_WB_.STR_PARAMETER));
        view.setFilter(new DDataFilter() {{
            this.add(new DDataFilter(Sample_WB_.STR_PARAMETER, DDataFilterOperator.LIKE, val));
        }});
        assertEquals(1, view.count());
    }

    @Test
    @Transactional
    // sql-operator LIKE --> SIMILAR TO
    public void likeExtTest() throws SQLException, IOException, DDataException {
        setUp();

        List<String> val = Arrays.asList("S1", "%2");
        List<Sample> st = iSampleRepository.listLikeExt(val);
        assertNotNull(st);
        assertEquals(2, st.size());
        // assertEquals(2,st.size());

        DDataView view = viewBuilder.build(Sample_WB_.class, new DDataFilter(Sample_WB_.STR_PARAMETER));
        view.setFilter(new DDataFilter() {{
            this.add(new DDataFilter(Sample_WB_.STR_PARAMETER, DDataFilterOperator.LIKE_IGNORE_CASE, val));
            this.add(new DDataFilter(Sample_WB_.LIST_PARAMETER) {{
                this.setNotExists(true);
                this.add(new DDataFilter(Inner_WB_.TEXT, DDataFilterOperator.LIKE, "rrrrrrrrrrrrrr"));
            }});
        }});
        view.select(0, 100);
        assertEquals(2, view.count());
    }

    //@Ignore
    @Test
    @Transactional
    public void cacheTest() throws SQLException, IOException {
        long t;
        setUp();

        Sample smpl = dData.getBeanRepository(Sample.class).get(1);//iSampleRepository.get(1);
        Inner inr = smpl.getInner();
        assertNotNull(inr);
        int inrId = inr.getId();
        DDataRepository<Inner, Integer> iInnerRepository = dData.getBeanRepository(Inner.class);
        assertEquals(this.iInnerRepository, iInnerRepository);

        for (int i = 0; i < 10000; i++) assertNotNull(iInnerRepository.get(inrId));

        Inner ni = iInnerRepository.create();
        ni.setSample(smpl);
        ni.setText("new");
        iInnerRepository.insert(ni);

        assertNotNull(iInnerRepository.get(ni.getId()));
    }

    @Autowired
    private DDataDictionary<SmallDict, Integer> smallDictRepo;
    @Autowired
    private DDataRepository<SmallDictGraf, SmallDictGraf_Key_> smallDictGrafRepository;

    @Test
    @Transactional
    public void testDictionaries() throws SQLException {
        setUp();

        // SELECT 2 records
        List<SmallDict> elts = smallDictRepo.list();

        int firstId = elts.get(0).getId();

        // NO ANY SELECT
        SmallDict e = smallDictRepo.get(firstId);
        assertNotNull(e);

        for (int i = 0; i < 100; i++) {
            // NO ANY SELECT
            assertTrue(smallDictRepo.list().stream().anyMatch(el -> (el.getId() == firstId)));
            // NO ANY SELECT
            assertNotNull(smallDictRepo.get(firstId));
        }

        e.setName("new name");
        //UPDATE 1 record
        smallDictRepo.update(e);

        //SELECT 2 records
        assertTrue(smallDictRepo.list().stream().anyMatch(el -> "new name".equals(el.getName())));
        // NO ANY SELECT
        e = smallDictRepo.get(1);
        assertEquals(2, e.getGraf().size());
        assertEquals(1, e.getTree().size());

        e = smallDictRepo.get(2);
        assertEquals(0, e.getGraf().size());
        e.setParentId(null);
        smallDictRepo.update(e);

        // insert child for 2
        smallDictGrafRepository.insert(new SmallDictGraf() {
            @Override
            public int getParent() {
                return 2;
            }

            @Override
            public void setParent(int id) {

            }

            @Override
            public Integer getChild() {
                return 1;
            }

            @Override
            public void setChild(Integer id) {

            }

            @Override
            public SmallDict getLinked() {
                return null;
            }
        });
        smallDictRepo.list(); // it read beans from select
        List<SmallDict> l = smallDictRepo.list(); // it read beans from cache
        e = l.stream().filter(b -> b.getId() == 2).findAny().orElse(null);
        assertNotNull(e);
        assertEquals(1, e.getGraf().size());
        e = l.stream().filter(b -> b.getId() == 1).findAny().orElse(null);
        assertNotNull(e);
        assertEquals(0, e.getTree().size());
    }

    @Test
    @org.junit.Ignore
    public void testDictionariesMultithread() throws SQLException, InterruptedException {
        setUp();
        smallDictRepo.list();

        ExecutorService tp = Executors.newFixedThreadPool(9);
        tp.submit(new DictionaryTest("test-1"));
        tp.submit(new DictionaryTest("test-2"));
        tp.submit(new DictionaryTest("test-3"));
        tp.submit(new DictionaryTest("test-4"));
        tp.submit(new DictionaryTest("test-5"));
        tp.submit(new DictionaryTest("test-6"));
        tp.submit(new DictionaryTest("test-7"));
        tp.submit(new DictionaryTest("test-8"));
        tp.submit(new DictionaryTest("test-9"));
        tp.shutdown();
        tp.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void beanSerializationTest() throws SQLException, IOException, ClassNotFoundException {
        setUp();

        assertNotNull(iSampleRepository);
        assertNotNull(iInnerRepository);

        Sample bean = iSampleRepository.get(1);
        assertNotNull(bean);
        assertEquals(1, bean.getId());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(bean);

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        Sample bean1 = (Sample) ois.readObject();
        List<? extends Inner> lp = bean1.getListParameter();
        assertNotNull(lp);
    }

    private class DictionaryTest implements Runnable {
        private final String name;
        private final Random r = new Random();

        DictionaryTest(String name) {
            this.name = name;
        }

        private void yeald() throws InterruptedException {
            int rnd = r.nextInt() & 0x3FF;
            Thread.sleep(rnd);
        }

        @Override
        public void run() {
            LOG.debug(name + " started");
            try {
                @SuppressWarnings("unchecked")
                DDataDictionary<SmallDict, Integer> smallDict = (DDataDictionary<SmallDict, Integer>)
                        springContext.getBean("smallDictRepository");

                SmallDict e1 = smallDict.get(1);

                yeald();
                e1.setName(name);
                smallDict.update(e1);
                yeald();

                e1 = smallDict.list().stream().filter(el -> el.getId() == 1).findAny().orElse(null);
                assert (e1 != null);
                LOG.debug(name + " read value as " + e1.getName());

                for (int i = 0; i < 5; i++) {
                    yeald();
                    smallDict.list();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOG.debug(name + " finished");
        }

    }

    @Test
    @Transactional
    @Ignore
    public void TestJAXB() throws SQLException, IOException, JAXBException {
        setUp();

        Sample smpl = iSampleRepository.get(1);
        assertNotNull(smpl);
        Inner inr = smpl.getInner();
        assertNotNull(inr);

        JAXBContext ctx = JAXBContext.newInstance(
                Sample.class.getPackage().getName()
        );
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        Marshaller marshaller = ctx.createMarshaller();
        marshaller.setProperty(javax.xml.bind.Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.setProperty(javax.xml.bind.Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
        StringWriter sw = new StringWriter();

        final String TEST_FOR_TEXT = "ttttttttt";
        smpl.setListParameter(new ArrayList<InnerImpl>() {{
            this.add(new InnerImpl() {{
                this.setId(10000);
                this.setText(TEST_FOR_TEXT);
            }});
            this.add(new InnerImpl() {{
                this.setId(10001);
                this.setText(TEST_FOR_TEXT);
            }});
        }});

        marshaller.marshal(smpl, sw);
        Sample deserialized = (Sample) unmarshaller.unmarshal(new StringReader(sw.toString()));
        assertNotNull(deserialized);
        //before creating package-info.java with @XmlSchema
        //XMLElement deserialized = (XMLElement) unmarshaller.unmarshal(new StringReader(sw.toString()));
        //assertEquals("Sample", deserialized.getName().getLocalPart());
        //assertTrue(deserialized.getValue() instanceof Sample);
        //Sample smpl1 = (Sample) deserialized.getValue();
        assertEquals(smpl, deserialized);
        assertEquals(inr, deserialized.getInner());
        assertEquals(TEST_FOR_TEXT, deserialized.getListParameter().get(0).getText());
        assertEquals(TEST_FOR_TEXT, deserialized.getListParameter().get(1).getText());
        //System.out.println(sw.toString());

        HistSample hs = iVSample.get(1);
        assertNotNull(hs);
        HistInner hi = hs.getInner();
        assertNotNull(hi);

        sw = new StringWriter();
        marshaller.marshal(hs, sw);
        HistSample hs_d = (HistSample) unmarshaller.unmarshal(new StringReader(sw.toString()));
        assertNotNull(hs_d);
        assertEquals(hi.getDateFrom(), hs_d.getInner().getDateFrom());
    }

    @Test
    @Transactional
    public void updateViaSaveToSampleKeyTest() throws SQLException {
        setUp();
        Sample sample = new SampleImpl();
        SampleRepository_Dao_ repository = dData.getRepository(SampleRepository_Dao_.class);
        UpdateOptions updateOptions = UpdateOptions.build()
                .exclude(Sample_WB_.ITEM)
                .exclude(Sample_WB_.LIST_PARAMETER)
                .exclude(Sample_WB_.INNER);
        sample.setId(1);
        sample.setStrParameter("update");
        dData.save(sample, updateOptions);
        assertTrue(repository.get(1).getStrParameter().equals("update"));

    }

    @Test
    @Transactional
    public void insertViaSaveToSampleKeyTest() throws SQLException, IOException {
        Sample sample = new SampleImpl();
        Inner inner = new InnerImpl();
        int sizeBeforeOfTable;
        int sizeAfterOfTable;
        SampleRepository_Dao_ repository = dData.getRepository(SampleRepository_Dao_.class);
        UpdateOptions updateOptions = UpdateOptions.build()
                .exclude(Sample_WB_.ITEM)
                .exclude(Sample_WB_.LIST_PARAMETER);
        setUp();
        sample.setId(5);
        sample.setStrParameter("insert");
        sample.setInner(inner);
        //inner.setId(1);

        sizeBeforeOfTable = repository.list(null, null, null, null, null, null).size();
        dData.save(sample, updateOptions);
        sizeAfterOfTable = repository.list(null, null, null, null, null, null).size();

        assertTrue(sizeAfterOfTable > sizeBeforeOfTable);
    }

    @Test
    @Transactional
    public void withAllAttributesSaveToSampleKeyTest() throws SQLException {
        setUp();
        String marker = "insert";
        Sample sample = new SampleImpl();
        Inner inner = new InnerImpl();
        UpdateOptions updateOptions = UpdateOptions.build()
                .includeXmlProps()
                .includeJsonProps();
        sample.setInner(inner);
        sample.setId(15);
        sample.setStrParameter(marker);
        sample.setListParameter(Collections.singletonList(inner));
        sample.setRemoteId(888);
        sample.setUuid(new UUID(1, 2));
        sample.setHash(new byte[]{1});
        ItemInnerImpl item = new ItemInnerImpl();
        ((SampleImpl) sample).setItem(item);
        inner.setSample(sample);
        inner.setSampleId(sample.getId());
        inner.setText("test");
        inner.setId(666);
        inner.setD1(777);
        Sample beamFromDB = dData.save(sample, updateOptions);
        assertTrue(beamFromDB.getStrParameter().equals(marker));

    }

    @Test
    @Transactional
    public void updateViaSaveToCompositeKeyTest() throws SQLException {
        setUp();
        CompositeKeyRepository_Dao_ repository = dData.getRepository(CompositeKeyRepository_Dao_.class);
        CompositeKeySample compositeKeySample = new CompositeKeySampleImpl();
        UpdateOptions updateOptions = UpdateOptions.build()
                .exclude(CompositeKeySample_WB_.INNER);

        CompositeKeySample_Key_ key =
                new CompositeKeySample_Key_(LocalDateTime.of(2017, 1, 1, 0, 0, 0), 1);
        compositeKeySample.setDateFrom(LocalDateTime.of(2017, 1, 1, 0, 0, 0));
        compositeKeySample.setId(1);
        compositeKeySample.setValue("update");
        dData.save(compositeKeySample, updateOptions);
        compositeKeySample = repository.get(key);
        assertTrue(compositeKeySample.getValue().equals("update"));
    }

    @Test
    @Transactional
    public void insertViaSaveCompositeKeyTest() throws SQLException {
        int sizeBeforeOfTable;
        int sizeAfterOfTable;
        CompositeKeyRepository_Dao_ repository = dData.getRepository(CompositeKeyRepository_Dao_.class);
        CompositeKeySample compositeKeySample = new CompositeKeySampleImpl();
        UpdateOptions updateOptions = UpdateOptions.build()
                .exclude(CompositeKeySample_WB_.INNER);
        setUp();

        compositeKeySample.setDateFrom(LocalDateTime.of(2018, 2, 2, 2, 2, 2));
        compositeKeySample.setId(5);
        compositeKeySample.setValue("insert");

        sizeBeforeOfTable = repository.list(null, null).size();
        dData.save(compositeKeySample, updateOptions);
        sizeAfterOfTable = repository.list(null, null).size();

        assertTrue(sizeAfterOfTable > sizeBeforeOfTable);
    }

    @Test
    @Transactional
    public void saveToSampleKeyWithExcludeTest() throws SQLException {
        setUp();
        SampleRepository_Dao_ repository = dData.getRepository(SampleRepository_Dao_.class);
        UpdateOptions updateOptions;
        SampleImpl sample = new SampleImpl();
        sample.setId(1);
        sample.setInnerId(1001);
        sample.setRemoteId(2);
        sample.setListParameter(Collections.emptyList());

        updateOptions = UpdateOptions.build()
                .exclude(Sample_WB_.LIST_PARAMETER)
                .exclude(Sample_WB_.ITEM)
                .exclude(Sample_WB_.INNER);
        sample.setStrParameter("update");
        dData.save(sample, updateOptions);
        assertTrue(repository.get(1).getStrParameter().equals("update"));

        updateOptions = updateOptions
                .exclude(Sample_WB_.LIST_PARAMETER)
                .exclude(Sample_WB_.ITEM)
                .exclude(Sample_WB_.INNER)
                .exclude(Sample_WB_.STR_PARAMETER);
        sample.setStrParameter("notUpdate");
        dData.save(sample, updateOptions);
        assertFalse(repository.get(1).getStrParameter().equals("notUpdate"));
    }

    @Test
    @Transactional
    public void saveToInnerBeanTest() throws SQLException {
        setUp();
        String marker = "yes";
        SampleRepository_Dao_ repository = dData.getRepository(SampleRepository_Dao_.class);
        Sample sample = new SampleImpl();
        InnerImpl inner = new InnerImpl();
        sample.setId(1);
        sample.setInnerId(1001);
        sample.setRemoteId(2);
        inner.setText(marker);
        inner.setId(1001);
        inner.setSampleId(1);
        sample.setInner(inner);
        inner.setSample(sample);
        UpdateOptions updateOptions = UpdateOptions.build()
                .exclude(Sample_WB_.LIST_PARAMETER)
                .exclude(Sample_WB_.ITEM)
                .exclude(Inner_WB_.V1)
                .includeXmlProps();
        dData.save(sample, updateOptions);
        String text = repository.get(1).getInner().getText();
        assertTrue(text.equals(marker));
    }

    @Test
    @Transactional
    @Ignore
    // больше не расчитывается что связаные бины будут удаляться если их связь 1 к 1 и не общий ключ
    public void DroppedViaSaveInnerBeanTest() throws SQLException {
        boolean hasInnerBean;
        SampleRepository_Dao_ repository = dData.getRepository(SampleRepository_Dao_.class);
        Sample sample = new SampleImpl();
        UpdateOptions updateOptions = UpdateOptions.build()
                .exclude(Sample_WB_.LIST_PARAMETER)
                .exclude(Sample_WB_.ITEM)
                .includeXmlProps();
        setUp();
        sample.setId(1);
        sample.setInnerId(1001);
        sample.setRemoteId(2);
        hasInnerBean = repository.get(1).getInner() != null;
        dData.save(sample, updateOptions);
        assertTrue(hasInnerBean && (repository.get(1).getInner() == null));
    }

    @Test
    @Transactional
    public void updateViaSaveToCollectionsInnerBeanTest() throws SQLException {
        setUp();
        Sample sample = new SampleImpl();
        sample.setId(1);
        Inner inner1 = new InnerImpl();
        inner1.setId(1001);
        inner1.setText("i1");
        inner1.setSampleId(1);
        inner1.setSample(sample);
        Inner inner2 = new InnerImpl();
        inner2.setSample(sample);
        inner2.setId(1004);
        inner2.setText("i4");
        inner2.setSampleId(1);
        ArrayList arrayList = new ArrayList();
        arrayList.add(inner1);
        arrayList.add(inner2);
        sample.setListParameter(arrayList);
        UpdateOptions updateOptions = UpdateOptions.build()
                .exclude(Inner_WB_.V1)
                .exclude(Sample_WB_.INNER)
                .includeXmlProps()
                .includeJsonProps();
        Sample save = dData.save(sample, updateOptions);
        assertTrue(save.getListParameter().size() == 2);
    }

    @Test
    @Transactional
    public void saveToEmptyCollectionsInnerBeanTest() throws SQLException {
        setUp();
        Sample sample = new SampleImpl();
        sample.setId(1);
        sample.setListParameter(null);
        UpdateOptions updateOptions = UpdateOptions.build()
                .exclude(Inner_WB_.V1)
                .exclude(Sample_WB_.INNER)
                .includeXmlProps()
                .includeJsonProps();
        Sample save = dData.save(sample, updateOptions);
        assertNull(save.getListParameter());
    }

    @Test
    @Transactional
    public void insertViaSaveToCollectionsInnerBeanTest() throws SQLException {
        setUp();
        Sample sample = new SampleImpl();
        sample.setId(3);
        Inner inner1 = new InnerImpl();
        inner1.setId(1001);
        inner1.setText("i1");
        inner1.setSample(sample);
        Inner inner2 = new InnerImpl();
        inner2.setSample(sample);
        inner2.setId(1004);
        inner2.setText("i4");
        ArrayList arrayList = new ArrayList();
        arrayList.add(inner1);
        arrayList.add(inner2);
        sample.setListParameter(arrayList);
        UpdateOptions updateOptions = UpdateOptions.build()
                .exclude(Sample_WB_.INNER)
                .exclude(Inner_WB_.V1)
                .exclude(Inner_WB_.D1);
        Sample save = dData.save(sample, updateOptions);
        assertTrue(save.getListParameter().size() == 2);
    }

    @Test
    @Transactional
    //пометить sample.inner как @XmlTransient
    public void includedXmlPropsTest() throws SQLException {
        setUp();
        SampleRepository_Dao_ repository = dData.getRepository(SampleRepository_Dao_.class);
        Sample sample = new SampleImpl();
        Inner inner = new InnerImpl();
        inner.setId(1001);
        inner.setText("test");
        inner.setSampleId(1);
        sample.setId(1);
        sample.setInner(inner);
        UpdateOptions updateOptions = UpdateOptions.build()
                .exclude(Inner_WB_.SAMPLE)
                .exclude(Sample_WB_.LIST_PARAMETER)
                .exclude(Inner_WB_.V1)
                .includeXmlProps();
        dData.save(sample, updateOptions);
        assertTrue(repository.get(1).getInner() != null);
    }

    @Test
    @Transactional
    public void registerOfUpdateOptionsTest() throws SQLException {
        setUp();

        SampleRepository_Dao_ repository = dData.getRepository(SampleRepository_Dao_.class);
        UpdateOptions updateOptions = UpdateOptions.build()
                .exclude(Sample_WB_.ITEM)
                .exclude(Sample_WB_.LIST_PARAMETER)
                .exclude(Sample_WB_.INNER);
        updateOptions.register(Inner_WB_.SAMPLE.getBeanInterface(), s -> ((Sample) s).setStrParameter("update"));
        Sample sample = new SampleImpl();
        sample.setId(1);
        sample.setStrParameter("hello");
        updateOptions.handledBean(sample);

        dData.save(sample, updateOptions);
        assertTrue(repository.get(1).getStrParameter().equals("update"));
    }


}