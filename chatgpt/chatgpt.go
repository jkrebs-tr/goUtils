package chatgpt

import (
	"fmt"

	"github.com/jkrebs-tr/goUtils/http"
)

// Send a request to ChatGPT and return the response - functions similarly to a normal chatGPT chat
//
// Parameters:
//   - model: The GPT model you want to use (gpt-4)
//   - messages: The messages/context to send to gpt
//   - tmp: The temperature for gpt (0 = detreministic | 1 = random)
//   - key: The openAPI key to use in the request
//
// Returns:
//   - Response: The chatGPT response with context and usage staticstics
//   - Error: Any errors that occur during execution
//
// Example Usage:
//
//	messages := []Message{
//		{Role: "system", Content: "You are a helpful assistant."},
//		{Role: "user", Content: "Tell me a joke."},
//	}
//
//	resp, err := SendRequest("gpt-4", messages, 0.7, os.Getenv("OPENAI_API_KEY"))
//	if err != nil {
//		log.Fatalf("Failed to get response from ChatGPT: %v", err)
//	}
//
//	fmt.Println("Assistant:", resp.Choices[0].Message.Content)
func SendRequest(model string, messages []Message, tmp float32, key string) (Response, error) {
	url := "https://api.openai.com/v1/chat/completions"
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", key),
		"Content-Type":  "application/json",
	}

	body := ChatRequest{
		Model:       model,
		Messages:    messages,
		Temperature: tmp,
	}

	var response Response
	err := http.MakeRequest("POST", url, &response, body, nil, headers)
	if err != nil {
		return Response{}, err
	}

	return response, nil
}
