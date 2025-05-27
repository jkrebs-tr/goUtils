package chatgpt

import (
	"fmt"

	"github.com/jkrebs-tr/goUtils/http"
)

func SendRequest(model string, messages []Message, tmp float32, key string) (Response, error) {
	url := "https://api.openai.com/v1/chat/completions"
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", key),
		"Content-Type": "application/json",
	}

	body := ChatRequest{
		Model: model,
		Messages: messages,
		Temperature: tmp,
	}

	var response Response
	err := http.MakeRequest("POST", url, &response, body, nil, headers)
	if err != nil {
		return Response{}, err
	}

	return response, nil
}
