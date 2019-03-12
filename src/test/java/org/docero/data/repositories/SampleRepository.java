package org.docero.data.repositories;

import org.apache.ibatis.session.RowBounds;
import org.docero.data.*;
import org.docero.data.beans.*;
import org.docero.data.beans.Sample;
import org.docero.data.utils.RowCounter;

import java.io.IOException;
import java.util.*;

@DDataRep("samples")
public interface SampleRepository extends DDataRepository<Sample, Integer> {

    @SampleRepository_DDataFetch_(DDataFetchType.LAZY)
    Sample get(@SampleRepository_Filter_(Sample_.ID) Integer id);

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
            RowCounter addCount, // for add  SELECT COUNT(*) method.
            DDataOrder<Sample_> sort,
            RowBounds bounds
    ) throws IOException, IllegalArgumentException;

    @SuppressWarnings("unused")
    default List<Sample> listOrdered() throws IOException {
        return list(null, null, null, null, null, new RowCounter(),
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
    @SampleRepository_DDataFetch_(value = DDataFetchType.COLLECTIONS_ARE_LAZY, eagerTrunkLevel = 1)
    List<Sample> listSimilarTo(
            @SampleRepository_Filter_(
                    value = Sample_.STR_PARAMETER,
                    option = DDataFilterOption.LIKE
            ) Collection<String> values
    );
    @SampleRepository_DDataFetch_(value = DDataFetchType.COLLECTIONS_ARE_LAZY, eagerTrunkLevel = 1)
    List<Sample> listLikeExt(
            @SampleRepository_Filter_(
                    value = Sample_.STR_PARAMETER,
                    option = DDataFilterOption.ILIKE
            ) Collection<String> values
    );

    List<Sample> listByUUid(@SampleRepository_Filter_(
            value = Sample_.UUID
    ) UUID uuid);
}
