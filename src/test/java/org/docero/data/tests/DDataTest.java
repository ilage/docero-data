package org.docero.data.tests;

import org.docero.data.DDataConfiguration;
import org.docero.data.DDataOrder;
import org.docero.data.DDataRepository;
import org.docero.data.DDataVersionalRepository;
import org.docero.data.beans.*;
import org.docero.data.repositories.CompositeKeyRepository;
import org.docero.data.repositories.SampleRepository;
import org.docero.data.repositories.VersionalSampleRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.Commit;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {
        TestsConfig.class,
        DDataConfiguration.class
})
@ActiveProfiles("test")
@SuppressWarnings("SpringJavaAutowiringInspection")
public class DDataTest {
    @Autowired
    DataSource dataSource;
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

    @SuppressWarnings("SqlNoDataSourceInspection")
    public void setUp() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement("" +
                    "DROP TABLE IF EXISTS ddata.\"sample\";" +
                    "CREATE TABLE ddata.\"sample\" (\n" +
                    "  id INT NOT NULL,\n" +
                    "  s VARCHAR(125),\n" +
                    "  i INT,\n" +
                    "  CONSTRAINT \"sample_pkey\" PRIMARY KEY (id)\n" +
                    ");\n" +
                    "\n" +
                    "DROP TABLE IF EXISTS ddata.\"inner\";" +
                    "CREATE TABLE ddata.\"inner\" (\n" +
                    "  id INT NOT NULL,\n" +
                    "  text VARCHAR(125),\n" +
                    "  sample_id INT,\n" +
                    "  CONSTRAINT \"inner_pkey\" PRIMARY KEY (id)\n" +
                    ");" +
                    "INSERT INTO ddata.\"sample\" (id, s, i) VALUES (1,'s1',1001);\n" +
                    "INSERT INTO ddata.\"sample\" (id, s, i) VALUES (2,'s2',1003);\n" +
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
                    "DROP SEQUENCE ddata.sample_seq;\n" +
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

        bean = iVSample.get(2, dateModifyInnerRecord);
        assertNotNull(bean);
        assertNotNull(bean.getInner());
        assertEquals(bean.getInner().getDateFrom(), dateModifyInnerRecord);
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
        sl = iSampleRepository.list(0, 10, null, null, null, null, null,
                DDataOrder.asc(Sample_.ID).addDesc(Sample_.INNER_ID));
        assertEquals(2, sl.size());

        sl = iSampleRepository.list(0, 10, null, null, null, null, 1002);
        assertEquals(0, sl.size());

        sl = iSampleRepository.list(0, 10, null, 1001, null, null, null);
        assertEquals(1, sl.size());

        sl = iSampleRepository.list(0, 10, null, null, null, "i1", null);
        assertEquals(1, sl.size());

        sl = iSampleRepository.list(0, 10, "s1", null, null, null, null);
        assertEquals(1, sl.size());

        sl = iSampleRepository.list(0, 10, null, null, "i2", null, null);
        assertEquals(1, sl.size());

        sl = iSampleRepository.list(0, 10, "s2", null, "i2", null, null);
        assertEquals(0, sl.size());


        Sample sample = iSampleRepository.create();
        sample.setStrParameter("a");
        sample.setId(3);
        sample.setInner(iInnerRepository.get(1002));
        iSampleRepository.insert(sample);

        sl = iSampleRepository.list(0, 10, null, null, null, null, 1002);
        assertEquals(1, sl.size());

        sample = iSampleRepository.get(sample.getId());
        assertEquals("a", sample.getStrParameter());

        iSampleRepository.delete(sample.getId());
        assertNull(iSampleRepository.get(sample.getId()));

        List<Sample> list = iSampleRepository.sampleProc(0);
        assertNotNull(list);
        assertEquals(2, list.size());
        assertNull(list.get(0).getListParameter());
        assertNotNull(iSampleRepository.get(list.get(0).getId()).getListParameter());

        assertEquals(2, iSampleRepository.count());

        assertEquals(1, iSampleRepository.listForInners(new HashSet<Integer>() {{
            this.add(1001);
            this.add(1002);
        }}).size());
        assertEquals(2, iSampleRepository.listForInners(new HashSet<Integer>() {{
            this.add(1001);
            this.add(1003);
        }}).size());

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

    //@Ignore
    @Test
    @Transactional
    public void cacheTest() throws SQLException, IOException {
        long t;
        setUp();

        Sample smpl = iSampleRepository.get(1);
        Inner inr = smpl.getInner();
        assertNotNull(inr);
        int inrId = inr.getId();

        for (int i = 0; i < 10000; i++) assertNotNull(iInnerRepository.get(inrId));

        Inner ni = iInnerRepository.create();
        ni.setId(1010);
        ni.setText("new");
        iInnerRepository.insert(ni);

        assertNotNull(iInnerRepository.get(1010));
    }
}