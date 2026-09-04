package main

import (
	"context"
	"errors"
	"math/rand/v2"
)

type Handler struct {
}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) Echo(ctx context.Context, text string) (string, error) {
	n := rand.IntN(10) //nolint: gosec
	if n == 0 {
		return "", errors.New(" it failed, no luck")
	}
	return text, nil
}
