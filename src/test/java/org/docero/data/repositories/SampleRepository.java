package org.docero.data.repositories;

import org.apache.ibatis.session.RowBounds;
import org.docero.data.*;
import org.docero.data.beans.*;
import org.docero.data.beans.Sample;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

@DDataRep("samples")
public interface SampleRepository extends DDataRepository<Sample, Integer> {

    @SampleRepository_DDataFetch_(DDataFetchType.LAZY)
    Sample get(@SampleRepository_Filter_(Sample_.ID) Integer id);

    @SampleRepository_DDataFetch_(value = DDataFetchType.COLLECTIONS_ARE_NO,
            select = "select * from ddata.sample_proc(:page)",
            resultMap = "org.docero.data.example.MyMapping.sampleProc"
    )
    List<Sample> sampleProc(int page) throws IOException;

    @SampleRepository_DDataFetch_(value = DDataFetchType.COLLECTIONS_ARE_LAZY, eagerTrunkLevel = 1)
        //tested: , ignore = {ISample_.INNER})
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
            ) String innerText, //inner text is
            @SampleRepository_Filter_(
                    Sample_.INNER_ID
            ) Integer inner, //inner_id is
            RowBounds bounds
    ) throws IOException, IllegalArgumentException;

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
            ) String innerText, //inner text is
            @SampleRepository_Filter_(
                    Sample_.INNER_ID
            ) Integer inner, //inner_id is
            DDataOrder<Sample_> sort,
            RowBounds bounds
    ) throws IOException, IllegalArgumentException;

    @SampleRepository_DDataFetch_(select = "SELECT COUNT(*) FROM \"ddata\".\"sample\"")
    long count();

    @SuppressWarnings("unused")
    default List<Sample> listOrdered() throws IOException {
        return list(null, null, null, null, null,
                DDataOrder.asc(Sample_.STR_PARAMETER), null);
    }

    List<Sample> listForInners(
            @SampleRepository_Filter_(
                    inner = Inner_.ID,
                    option = DDataFilterOption.IN
            ) HashSet<Integer> values);

    List<Sample> testForNull(
            @SampleRepository_Filter_(
                    value = Sample_.INNER_ID,
                    option = DDataFilterOption.IS_NULL
            ) boolean innerIsNull);

    Sample getOneBy(
            @SampleRepository_Filter_(
                    value = Sample_.STR_PARAMETER,
                    option = DDataFilterOption.LIKE
            ) String val,
            @SampleRepository_Filter_(
                    listParameter = Inner_.ID
            ) Integer listId
    );

    List<Sample> listByUUid(@SampleRepository_Filter_(
            value = Sample_.UUID
    ) UUID uuid);
}
