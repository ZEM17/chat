package db

import (
	"database/sql"
	"fmt"
	"log"

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
