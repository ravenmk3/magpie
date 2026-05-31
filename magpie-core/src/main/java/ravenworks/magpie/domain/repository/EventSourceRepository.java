package ravenworks.magpie.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ravenworks.magpie.domain.entity.EventSourceEntity;


public interface EventSourceRepository extends JpaRepository<EventSourceEntity, String> {

}
