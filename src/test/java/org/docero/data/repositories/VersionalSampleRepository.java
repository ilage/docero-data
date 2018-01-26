package org.docero.data.repositories;

import org.docero.data.DDataRep;
import org.docero.data.DDataVersionalRepository;
import org.docero.data.beans.HistSample;
import org.docero.data.beans.HistSample_;

import java.time.LocalDateTime;
import java.util.List;

@DDataRep
public interface VersionalSampleRepository extends DDataVersionalRepository<HistSample, Integer, LocalDateTime> {
    List<HistSample> list(
            @VersionalSampleRepository_Filter_(
                    HistSample_.ID
            ) int id);

    List<HistSample> listAt(
            @VersionalSampleRepository_Filter_(
                    HistSample_.ID
            ) int i,
            @VersionalSampleRepository_Filter_(
                    HistSample_.VERSION_
            ) LocalDateTime atTime);
}
