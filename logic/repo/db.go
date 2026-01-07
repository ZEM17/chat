package repo

import (
	"database/sql" // *** 新增: 用于 sql.NullInt64 ***
	"errors"
	"fmt"
	"log"
	"strconv" // *** 新增: 用于 ID 转换 ***
	"strings"

	_ "github.com/go-sql-driver/mysql" // 导入 MySQL 驱动
	"golang.org/x/crypto/bcrypt"
)

var DB *sql.DB

var ErrUserNotFound = errors.New("user not found")

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

// CheckUserExists 检查一个用户 ID 是否存在
// (这是一个辅助函数，用于被 SaveMessage 调用)
func CheckUserExists(userID int64) (bool, error) {
	var exists int
	// 我们在 ValidateUser 之后才使用它，所以只查 ID
	err := DB.QueryRow("SELECT COUNT(1) FROM users WHERE id = ?", userID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check user existence (ID: %d): %v", userID, err)
	}
	return exists > 0, nil
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

func SaveMessage(fromUserID, toUserID, toGroupID string, payload []byte, msgType string) (int64, error) {
	// 1. 验证发送者 (FromUserID)
	fromID, err := strconv.ParseInt(fromUserID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid from_user_id: %v", err)
	}

	// (JWT 应该已经保证了 fromID 是存在的，但作为数据库的最后防线，检查一下是好的)
	exists, err := CheckUserExists(fromID)
	if err != nil {
		return 0, err // 数据库查询错误
	}
	if !exists {
		// 使用 %w 来包装我们的特定错误
		return 0, fmt.Errorf("sender user (ID: %d) %w", fromID, ErrUserNotFound)
	}

	// 2. 验证接收者 (ToUserID 或 ToGroupID)
	var toID sql.NullInt64
	var groupID sql.NullInt64

	if msgType == "private" {
		if toUserID == "" {
			return 0, fmt.Errorf("to_user_id is required for private message")
		}
		tid, err := strconv.ParseInt(toUserID, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid to_user_id: %v", err)
		}

		// *** 核心验证 ***
		exists, err := CheckUserExists(tid)
		if err != nil {
			return 0, err // 数据库查询错误
		}
		if !exists {
			// *** 这就是我们想要的：返回特定错误 ***
			return 0, fmt.Errorf("recipient user (ID: %d) %w", tid, ErrUserNotFound)
		}

		toID = sql.NullInt64{Int64: tid, Valid: true}

	} else if msgType == "group" {
		if toGroupID == "" {
			return 0, fmt.Errorf("to_group_id is required for group message")
		}
		gid, err := strconv.ParseInt(toGroupID, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid to_group_id: %v", err)
		}

		// TODO: 在未来, 这里应该检查 'groups' 表
		// exists, err := CheckGroupExists(gid)
		// ...

		groupID = sql.NullInt64{Int64: gid, Valid: true}
	} else {
		return 0, fmt.Errorf("unknown message type: %s", msgType)
	}

	// 3. 插入数据库
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
