package org.docero.data.example;

import org.docero.data.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

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
            ) Integer inner //inner_id is
    ) throws IOException, IllegalArgumentException;

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

    @SuppressWarnings("unused")
    default List<Sample> listOrdered() throws IOException {
        return list(0, 0, null, null, null, null, null,
                DDataOrder.asc(Sample_.STR_PARAMETER));
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
}
