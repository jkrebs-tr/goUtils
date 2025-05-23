package ses

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ses"
)

type EmailConfig struct {
	Recipients []string
	Sender     string
	Subject    string
	HTMLBody   string
	TextBody   string
	Region     string
}

type SESClient struct {
	svc *ses.SES
}

// NewSESClient creates a new SES client configured for the specified AWS region.
// Uses default AWS credential chain (environment variables, ~/.aws/credentials, IAM roles, etc.).
//
// Parameters:
//   - region: AWS region where SES is configured (e.g., "us-east-1", "eu-west-1").
//     If empty string is provided, defaults to "us-east-1".
//
// Returns a configured SESClient and any error encountered during AWS session creation.
//
// Example usage:
//
//	// Create client with specific region
//	client, err := NewSESClient("us-west-2")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Create client with default region (us-east-1)
//	client, err := NewSESClient("")
func NewSESClient(region string) (*SESClient, error) {
	if region == "" {
		region = "us-east-1" // Default region
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %v", err)
	}

	return &SESClient{
		svc: ses.New(sess),
	}, nil
}

// SendEmail sends a single email using AWS SES with the provided configuration.
// Validates required fields and supports both HTML and plain text email bodies.
//
// Parameters:
//   - config: EmailConfig struct containing all email parameters
//
// Required fields in config:
//   - Recipients: At least one valid email address
//   - Sender: Must be a verified email address in AWS SES
//   - Subject: Cannot be empty
//   - HTMLBody or TextBody: At least one must be provided
//
// Returns an error if validation fails, AWS SES call fails, or email cannot be sent.
// On success, prints the AWS message ID to stdout for tracking purposes.
//
// Example usage:
//
//	config := EmailConfig{
//		Recipients: []string{"user@example.com", "admin@example.com"},
//		Sender:     "noreply@yourdomain.com",
//		Subject:    "Welcome to our service",
//		HTMLBody:   "<h1>Welcome!</h1><p>Thanks for signing up.</p>",
//		TextBody:   "Welcome!\n\nThanks for signing up.",
//	}
//	err := client.SendEmail(config)
func (c *SESClient) SendEmail(config EmailConfig) error {
	if len(config.Recipients) == 0 {
		return fmt.Errorf("no recipients specified")
	}
	if config.Sender == "" {
		return fmt.Errorf("sender email is required")
	}
	if config.Subject == "" {
		return fmt.Errorf("subject is required")
	}
	if config.HTMLBody == "" && config.TextBody == "" {
		return fmt.Errorf("either HTML body or text body is required")
	}

	message := &ses.Message{
		Subject: &ses.Content{
			Data: aws.String(config.Subject),
		},
		Body: &ses.Body{},
	}

	if config.HTMLBody != "" {
		message.Body.Html = &ses.Content{
			Data: aws.String(config.HTMLBody),
		}
	}

	if config.TextBody != "" {
		message.Body.Text = &ses.Content{
			Data: aws.String(config.TextBody),
		}
	}

	input := &ses.SendEmailInput{
		Destination: &ses.Destination{
			ToAddresses: aws.StringSlice(config.Recipients),
		},
		Message: message,
		Source:  aws.String(config.Sender),
	}

	result, err := c.svc.SendEmail(input)
	if err != nil {
		return fmt.Errorf("failed to send email: %v", err)
	}

	fmt.Printf("Email sent successfully. Message ID: %s\n", *result.MessageId)

	return nil
}

// SendEmailBulk sends multiple emails with different configurations in sequence.
// Each email is sent individually, allowing for personalized content per recipient.
// If any email fails, the function continues sending remaining emails and returns
// a combined error with details of all failures.
//
// Parameters:
//   - configs: Slice of EmailConfig structs, each representing a separate email to send
//
// Returns an error containing details of any failed email sends. If all emails
// send successfully, returns nil. Failed emails are identified by their index
// in the configs slice.
//
// Example usage:
//
//	configs := []EmailConfig{
//		{
//			Recipients: []string{"user1@example.com"},
//			Sender:     "noreply@yourdomain.com",
//			Subject:    "Personal message for User 1",
//			HTMLBody:   "<p>Hello User 1!</p>",
//		},
//		{
//			Recipients: []string{"user2@example.com"},
//			Sender:     "noreply@yourdomain.com",
//			Subject:    "Personal message for User 2",
//			HTMLBody:   "<p>Hello User 2!</p>",
//		},
//	}
//	err := client.SendEmailBulk(configs)
func (c *SESClient) SendEmailBulk(configs []EmailConfig) error {
	var errors []string

	for i, config := range configs {
		if err := c.SendEmail(config); err != nil {
			errors = append(errors, fmt.Sprintf("email %d: %v", i, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to send %d emails: %v", len(errors), errors)
	}

	return nil
}

// SendSimpleEmail is a convenience function for sending a single email without
// creating an SESClient instance first. Creates a temporary client with default
// region (us-east-1) and sends the email immediately.
//
// Parameters:
//   - recipients: Slice of recipient email addresses
//   - sender: Sender email address (must be verified in AWS SES)
//   - subject: Email subject line
//   - htmlBody: HTML formatted email body (can be empty if textBody provided)
//   - textBody: Plain text email body (can be empty if htmlBody provided)
//
// Returns an error if client creation fails or email sending fails.
// At least one of htmlBody or textBody must be non-empty.
//
// Example usage:
//
//	// Send HTML email
//	recipients := []string{"user@example.com"}
//	err := SendSimpleEmail(
//		recipients,
//		"noreply@yourdomain.com",
//		"Test Subject",
//		"<h1>Hello World!</h1>",
//		"Hello World!",
//	)
//
//	// Send text-only email
//	err := SendSimpleEmail(
//		recipients,
//		"noreply@yourdomain.com",
//		"Test Subject",
//		"", // empty HTML body
//		"Hello World!",
//	)
func SendSimpleEmail(recipients []string, sender, subject, htmlBody, textBody string) error {
	client, err := NewSESClient("")
	if err != nil {
		return err
	}

	config := EmailConfig{
		Recipients: recipients,
		Sender:     sender,
		Subject:    subject,
		HTMLBody:   htmlBody,
		TextBody:   textBody,
	}

	return client.SendEmail(config)
}
