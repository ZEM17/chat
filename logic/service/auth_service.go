package service

import (
	"chat/common/pb"
	"chat/logic/repo"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type AuthService struct {
	pb.UnimplementedAuthServiceServer
}

func NewAuthService() *AuthService {
	return &AuthService{}
}

// TODO: move to config
var jwtKey = []byte("my_secret_key")

type Claims struct {
	UserID   string `json:"user_id"`
	Nickname string `json:"nickname"`
	jwt.RegisteredClaims
}

func (s *AuthService) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if req.Username == "" || req.Password == "" {
		return nil, fmt.Errorf("username and password required")
	}
	if req.Nickname == "" {
		req.Nickname = req.Username
	}

	id, err := repo.CreateUser(req.Username, req.Password, req.Nickname)
	if err != nil {
		return nil, err
	}

	return &pb.RegisterResponse{
		Message: "User created successfully",
		UserId:  id,
	}, nil
}

func (s *AuthService) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
	id, nickname, err := repo.ValidateUser(req.Username, req.Password)
	if err != nil {
		return nil, err
	}

	userIDStr := strconv.FormatInt(id, 10)

	expirationTime := time.Now().Add(24 * time.Hour)
	claims := &Claims{
		UserID:   userIDStr,
		Nickname: nickname,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(jwtKey)
	if err != nil {
		return nil, err
	}

	return &pb.LoginResponse{
		Token:    tokenString,
		UserId:   userIDStr,
		Nickname: nickname,
	}, nil
}

func (s *AuthService) ValidateToken(ctx context.Context, req *pb.ValidateTokenRequest) (*pb.ValidateTokenResponse, error) {
	tokenStr := req.Token
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (interface{}, error) {
		return jwtKey, nil
	})

	if err != nil || !token.Valid {
		return &pb.ValidateTokenResponse{
			Valid:    false,
			ErrorMsg: "invalid token",
		}, nil
	}

	return &pb.ValidateTokenResponse{
		Valid:    true,
		UserId:   claims.UserID,
		Nickname: claims.Nickname,
	}, nil
}
