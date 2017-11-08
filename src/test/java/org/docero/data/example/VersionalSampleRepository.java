package org.docero.data.example;

import org.docero.data.DDataRep;
import org.docero.data.DDataVersionalRepository;

import java.time.LocalDateTime;
import java.util.List;

@DDataRep
public interface VersionalSampleRepository extends DDataVersionalRepository<HistSample, Integer, LocalDateTime> {
    List<HistSample> list(
            @VersionalSampleRepository_Filter_(
                    HistSample_.ID
            ) int id);
}
