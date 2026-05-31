package ravenworks.magpie.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ravenworks.magpie.domain.entity.SinkEntity;


public interface SinkRepository extends JpaRepository<SinkEntity, String> {

}
