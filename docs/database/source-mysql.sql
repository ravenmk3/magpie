CREATE TABLE IF NOT EXISTS `magpie_outbox_message`
(
    `id`           CHAR(32)     NOT NULL COMMENT 'ID',
    `type`         VARCHAR(128) NOT NULL DEFAULT '' COMMENT '消息类型',
    `event_time`   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '发生时间',
    `topic`        VARCHAR(128) NOT NULL DEFAULT '' COMMENT '消息主题',
    `tenant_id`    VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '租户 ID',
    `business_key` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '业务键',
    `headers`      JSON         NOT NULL COMMENT '消息头',
    `payload`      MEDIUMTEXT   NOT NULL COMMENT '消息体',

    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    CHARSET = utf8mb4
    COMMENT '待发布消息';
