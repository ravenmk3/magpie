package ravenworks.magpie.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ravenworks.magpie.domain.entity.LeaderLockEntity;

import java.time.LocalDateTime;


public interface LeaderLockRepository extends JpaRepository<LeaderLockEntity, Integer> {

    @Modifying
    @Query(value = "UPDATE magpie_leader_lock SET heartbeat_at = :now WHERE id = 1 AND instance_id = :instanceId", nativeQuery = true)
    int renewHeartbeat(@Param("instanceId") String instanceId, @Param("now") LocalDateTime now);

    @Modifying
    @Query(value = "UPDATE magpie_leader_lock SET instance_id = '', acquired_at = '1970-01-01 00:00:00.000', heartbeat_at = '1970-01-01 00:00:00.000' WHERE id = 1 AND instance_id = :instanceId", nativeQuery = true)
    int releaseLock(@Param("instanceId") String instanceId);

}
