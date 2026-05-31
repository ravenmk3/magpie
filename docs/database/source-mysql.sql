CREATE TABLE IF NOT EXISTS `magpie_outbox_event`
(
    `id`            CHAR(32)     NOT NULL COMMENT 'ID',
    `type`          VARCHAR(128) NOT NULL DEFAULT '' COMMENT '事件类型',
    `time`          DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '发生时间',
    `tenant_id`     VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '租户 ID',
    `topic`         VARCHAR(128) NOT NULL DEFAULT '' COMMENT '消息主题',
    `partition_key` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '分区键',
    `headers`       JSON         NOT NULL COMMENT '消息头',
    `payload`       MEDIUMTEXT   NOT NULL COMMENT '消息体',

    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    CHARSET = utf8mb4
    COMMENT '待发布事件';
