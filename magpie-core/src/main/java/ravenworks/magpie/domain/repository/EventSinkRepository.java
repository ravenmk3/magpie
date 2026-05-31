package ravenworks.magpie.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ravenworks.magpie.domain.entity.EventSinkEntity;


public interface EventSinkRepository extends JpaRepository<EventSinkEntity, String> {

}
