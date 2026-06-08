package ravenworks.magpie.domain.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(name = "magpie_sink_offset")
public class SinkOffsetEntity implements Serializable {

    @Id
    @Column(name = "id", nullable = false, length = 128)
    private String id;

    @Column(name = "target", nullable = false, length = 128)
    private String target;

    @Column(name = "`partition`", nullable = false)
    private int partition;

    @Column(name = "`offset`", nullable = false)
    private long offset;

    @Version
    @Column(name = "version", nullable = false)
    private int version;

    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false, insertable = false, updatable = false)
    private LocalDateTime updatedAt;

    public SinkOffsetEntity() {
    }

    public SinkOffsetEntity(String id, String target, int partition, long offset) {
        this.id = id;
        this.target = target;
        this.partition = partition;
        this.offset = offset;
    }

}
