-- 创建 users 表，符合大纲 5.1
CREATE TABLE IF NOT EXISTS `users` (
  `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `username` VARCHAR(64) NOT NULL UNIQUE,
  `password_hash` VARCHAR(255) NOT NULL,
  `nickname` VARCHAR(64),
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 消息历史记录表 (对应大纲 4.2.4)
-- 用于存储所有聊天记录
CREATE TABLE IF NOT EXISTS `messages` (
  `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `from_user_id` BIGINT NOT NULL COMMENT '发送者 User ID',
  `to_user_id` BIGINT COMMENT '接收者 User ID (单聊时)',
  `to_group_id` BIGINT COMMENT '接收群组 ID (群聊时)',
  `content` TEXT COMMENT '消息内容 (通常是客户端发来的原始 JSON payload)',
  `type` VARCHAR(16) NOT NULL COMMENT '消息类型: private, group',
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_from_user_id` (`from_user_id`),
  INDEX `idx_to_user_id` (`to_user_id`),
  INDEX `idx_to_group_id` (`to_group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 离线消息表 (对应大纲 4.2.5)
-- 仅在接收者不在线时存储
CREATE TABLE IF NOT EXISTS `offline_messages` (
  `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `user_id` BIGINT NOT NULL COMMENT '消息接收者 User ID',
  `message_data` JSON COMMENT '存储完整的 UpstreamMessage (来自 d.Body)',
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;