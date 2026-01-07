package service

import (
	"chat/common/pb"
	"chat/logic/repo"
	"context"
	"fmt"
	"strconv"
)

type GroupService struct {
	pb.UnimplementedGroupServiceServer
}

func NewGroupService() *GroupService {
	return &GroupService{}
}

func (s *GroupService) CreateGroup(ctx context.Context, req *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
	ownerID, err := strconv.ParseInt(req.OwnerId, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid owner_id")
	}

	groupID, err := repo.CreateGroup(req.Name, ownerID)
	if err != nil {
		return nil, err
	}

	// Owner auto joins
	repo.JoinGroup(groupID, ownerID)

	return &pb.CreateGroupResponse{
		GroupId: groupID,
		Message: "Group created successfully",
	}, nil
}

func (s *GroupService) JoinGroup(ctx context.Context, req *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
	userID, err := strconv.ParseInt(req.UserId, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid user_id")
	}

	err = repo.JoinGroup(req.GroupId, userID)
	if err != nil {
		return nil, err
	}

	return &pb.JoinGroupResponse{
		Message: "Joined group successfully",
	}, nil
}

func (s *GroupService) GetGroupMembers(ctx context.Context, req *pb.GetGroupMembersRequest) (*pb.GetGroupMembersResponse, error) {
	members, err := repo.GetGroupMembers(req.GroupId)
	if err != nil {
		return nil, err
	}

	var pbMembers []*pb.Member
	for _, uid := range members {
		pbMembers = append(pbMembers, &pb.Member{
			UserId:   fmt.Sprintf("%d", uid),
			Nickname: fmt.Sprintf("User%d", uid), // Placeholder
			Role:     "member",
		})
	}

	return &pb.GetGroupMembersResponse{
		Members: pbMembers,
	}, nil
}
