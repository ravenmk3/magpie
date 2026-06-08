package ravenworks.magpie.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ravenworks.magpie.domain.entity.SinkOffsetEntity;

public interface SinkOffsetRepository extends JpaRepository<SinkOffsetEntity, String> {

    @Modifying
    @Query(value = "UPDATE magpie_sink_offset SET `offset` = :offset, version = version + 1 WHERE id = :id", nativeQuery = true)
    int updateOffset(@Param("id") String id, @Param("offset") long offset);

}
