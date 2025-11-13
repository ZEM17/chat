package db

import (
	"database/sql" // *** 新增: 用于 sql.NullInt64 ***
	"fmt"
	"log"
	"strconv" // *** 新增: 用于 ID 转换 ***
	"strings"

	_ "github.com/go-sql-driver/mysql" // 导入 MySQL 驱动
	"golang.org/x/crypto/bcrypt"
)

var DB *sql.DB

// InitDB 初始化数据库连接
func InitDB(dataSourceName string) {
	var err error
	DB, err = sql.Open("mysql", dataSourceName)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	if err = DB.Ping(); err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	log.Println("Database connected.")
}

// HashPassword 使用 bcrypt 对密码进行哈希
func HashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), 14)
	return string(bytes), err
}

// CheckPasswordHash 验证密码哈希
func CheckPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

// CreateUser 创建一个新用户
func CreateUser(username, password, nickname string) (int64, error) {
	hashedPassword, err := HashPassword(password)
	if err != nil {
		return 0, fmt.Errorf("failed to hash password: %v", err)
	}

	// 检查用户名是否已存在
	var exists int
	err = DB.QueryRow("SELECT COUNT(1) FROM users WHERE username = ?", username).Scan(&exists)
	if err != nil {
		return 0, fmt.Errorf("failed to check username existence: %v", err)
	}
	if exists > 0 {
		return 0, fmt.Errorf("username '%s' already exists", username)
	}

	// 插入新用户
	res, err := DB.Exec("INSERT INTO users (username, password_hash, nickname) VALUES (?, ?, ?)",
		username, hashedPassword, nickname)
	if err != nil {
		return 0, fmt.Errorf("failed to insert user: %v", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get last insert ID: %v", err)
	}
	return id, nil
}

// ValidateUser 验证用户名和密码，成功则返回用户 ID
func ValidateUser(username, password string) (int64, string, error) {
	var id int64
	var nickname string
	var hashedPassword string

	err := DB.QueryRow("SELECT id, nickname, password_hash FROM users WHERE username = ?", username).Scan(&id, &nickname, &hashedPassword)
	if err == sql.ErrNoRows {
		return 0, "", fmt.Errorf("user not found")
	}
	if err != nil {
		return 0, "", fmt.Errorf("database error: %v", err)
	}

	if CheckPasswordHash(password, hashedPassword) {
		return id, nickname, nil // 验证成功
	}

	return 0, "", fmt.Errorf("invalid password")
}

// *** (新增) ***
// SaveMessage 持久化聊天记录
func SaveMessage(fromUserID, toUserID, toGroupID string, payload []byte, msgType string) (int64, error) {
	// 我们的业务逻辑使用字符串 ID, 但数据库存储 BIGINT
	fromID, err := strconv.ParseInt(fromUserID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid from_user_id: %v", err)
	}

	// 处理可能的空 ID (单聊时 toGroupID 为空, 群聊时 toUserID 可能为空)
	var toID sql.NullInt64
	if toUserID != "" {
		tid, err := strconv.ParseInt(toUserID, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid to_user_id: %v", err)
		}
		toID = sql.NullInt64{Int64: tid, Valid: true}
	}

	var groupID sql.NullInt64
	if toGroupID != "" {
		gid, err := strconv.ParseInt(toGroupID, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid to_group_id: %v", err)
		}
		groupID = sql.NullInt64{Int64: gid, Valid: true}
	}

	// content 存储的是原始的 payload (通常是客户端发来的 JSON)
	res, err := DB.Exec(
		"INSERT INTO messages (from_user_id, to_user_id, to_group_id, content, type) VALUES (?, ?, ?, ?, ?)",
		fromID, toID, groupID, string(payload), msgType,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to insert message: %v", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get last insert ID for message: %v", err)
	}
	return id, nil
}

// *** (新增) ***
// SaveOfflineMessage 存储离线消息
func SaveOfflineMessage(toUserID string, messageData []byte) error {
	toID, err := strconv.ParseInt(toUserID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid to_user_id for offline message: %v", err)
	}

	// message_data 存储的是 UpstreamMessage 的 JSON (来自 d.Body)
	_, err = DB.Exec(
		"INSERT INTO offline_messages (user_id, message_data) VALUES (?, ?)",
		toID, messageData,
	)
	if err != nil {
		return fmt.Errorf("failed to insert offline message: %v", err)
	}
	return nil
}

// *** (新增) ***
// GetOfflineMessages 在事务中获取并锁定指定用户的离线消息
// 返回消息 ID 列表和消息数据 (UpstreamMessage JSON) 列表
func GetOfflineMessages(tx *sql.Tx, userID string) ([]int64, [][]byte, error) {
	uID, err := strconv.ParseInt(userID, 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid user_id: %v", err)
	}

	// 锁定行以防止其他事务处理
	rows, err := tx.Query("SELECT id, message_data FROM offline_messages WHERE user_id = ? ORDER BY created_at ASC FOR UPDATE", uID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query offline messages: %v", err)
	}
	defer rows.Close()

	var ids []int64
	var messages [][]byte
	for rows.Next() {
		var id int64
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, nil, fmt.Errorf("failed to scan offline message: %v", err)
		}
		ids = append(ids, id)
		messages = append(messages, data)
	}
	return ids, messages, nil
}

// *** (新增) ***
// ClearOfflineMessages 在事务中根据 ID 列表删除离线消息
func ClearOfflineMessages(tx *sql.Tx, ids []int64) error {
	if len(ids) == 0 {
		return nil // 没有要删除的
	}

	// 构建 'DELETE ... WHERE id IN (?, ?, ?)'
	query := "DELETE FROM offline_messages WHERE id IN (?" + strings.Repeat(",?", len(ids)-1) + ")"

	// 将 []int64 转换为 []interface{}
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}

	_, err := tx.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to delete offline messages: %v", err)
	}
	return nil
}
