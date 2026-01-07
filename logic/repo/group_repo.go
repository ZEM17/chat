package repo

import (
	"database/sql"
	"time"
)

// Group 结构体
type Group struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	OwnerID   int64     `json:"owner_id"`
	CreatedAt time.Time `json:"created_at"`
}

// CreateGroup 创建群组
func CreateGroup(name string, ownerID int64) (int64, error) {
	res, err := DB.Exec("INSERT INTO `groups` (name, owner_id) VALUES (?, ?)", name, ownerID)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// JoinGroup 加入群组
func JoinGroup(groupID, userID int64) error {
	// Check if already in group
	var exists int
	err := DB.QueryRow("SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?", groupID, userID).Scan(&exists)
	if err == nil {
		return nil // Already joined
	}

	_, err = DB.Exec("INSERT INTO group_members (group_id, user_id) VALUES (?, ?)", groupID, userID)
	return err
}

// GetGroupMembers 获取群成员ID列表
func GetGroupMembers(groupID int64) ([]int64, error) {
	rows, err := DB.Query("SELECT user_id FROM group_members WHERE group_id = ?", groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var userIDs []int64
	for rows.Next() {
		var uid int64
		if err := rows.Scan(&uid); err != nil {
			continue
		}
		userIDs = append(userIDs, uid)
	}
	return userIDs, nil
}

// CheckGroupExists 检查群是否存在
func CheckGroupExists(groupID int64) (bool, error) {
	var exists int
	err := DB.QueryRow("SELECT 1 FROM `groups` WHERE id = ?", groupID).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
