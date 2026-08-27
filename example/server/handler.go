package main

import "context"

type Handler struct {
}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) Echo(ctx context.Context, text string) (string, error) {
	return text, nil
}
