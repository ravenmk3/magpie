CREATE TABLE IF NOT EXISTS `magpie_leader_lock`
(
    `id`           INT         NOT NULL DEFAULT 1,
    `instance_id`  VARCHAR(32) NOT NULL COMMENT '持有锁的实例 UUID',
    `acquired_at`  DATETIME(3) NOT NULL COMMENT '获取锁时间',
    `heartbeat_at` DATETIME(3) NOT NULL COMMENT '最后心跳时间',

    PRIMARY KEY (`id`),
    CONSTRAINT `chk_single_row` CHECK (`id` = 1)
)
    ENGINE = InnoDB
    CHARSET = utf8mb4
    COMMENT 'Leader 锁';
