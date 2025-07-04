package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/stripe/stripe-go/v82"
)

type Stripe struct {
	client      *stripe.Client
	app         *application
	propertyMap map[string]string
}

func (app *application) newStripe(systemInfo SystemInfo) (system System, err error) {

	stripeClient := stripe.NewClient(systemInfo.ApiKey)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	listParams := &stripe.ProductListParams{}
	listParams.Limit = stripe.Int64(1)

	for _, err := range stripeClient.V1Products.List(ctx, listParams) {
		// We are only testing the connection, so we don't want to do anything with
		// the data. Do not read it, store it, print it, or log it. Nothing!
		if err != nil {
			return nil, err
		}
	}

	if systemInfo.UseCliListener {

		if _, err := exec.LookPath("stripe"); err != nil {
			return nil, fmt.Errorf("Stripe CLI not found in PATH. Please install it to use Stripe listening mode: %w", err)
		}

		// Forward Stripe events to our local endpoint
		forwardURL := fmt.Sprintf("http://localhost:%d/%v", app.config.port, systemInfo.Name)
		cmd := exec.Command("stripe", "listen", "--forward-to", forwardURL)
		// cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		// cmd.Stdin = os.Stdin

		cmd.Env = append(os.Environ(), fmt.Sprintf("STRIPE_API_KEY=%s", systemInfo.ApiKey))

		go func() {
			fmt.Println("Starting Stripe CLI listener")
			err := cmd.Run()
			if err != nil {
				return
			}
		}()
	}

	stripeCmd := exec.Command("stripe", "trigger", "product.created")
	// stripeCmd.Stdout = os.Stdout
	stripeCmd.Stderr = os.Stderr
	stripeCmd.Env = append(os.Environ(), fmt.Sprintf("STRIPE_API_KEY=%s", systemInfo.ApiKey))
	err = stripeCmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run stripe trigger: %w", err)
	}

	// Block until we receive something on receiverWebhookCh or timeout after 5 seconds
	// select {
	// case <-receiverWebhookCh:
	// 	// Received signal, proceed with Stripe system initialization
	// 	fmt.Println("Proceeding with Stripe system initialization.")
	// case <-time.After(5 * time.Second):
	// 	// Timeout after 5 seconds
	// 	return nil, fmt.Errorf("timeout waiting for Stripe webhook healthcheck")
	// }

	// Register the Stripe webhook handler
	stripeSystem := &Stripe{
		client: stripeClient,
		app:    app,
	}

	return stripeSystem, nil
}

// func stripeWebhookHealthcheck(w http.ResponseWriter, r *http.Request) {
// 	fmt.Println("Stripe healthcheck received")
// 	_, err := io.ReadAll(r.Body)
// 	if err != nil {
// 		http.Error(w, "failed to read request body", http.StatusInternalServerError)
// 		return
// 	}
// 	select {
// 	case receiverWebhookCh <- struct{}{}:
// 	default:
// 	}
// 	w.WriteHeader(http.StatusOK)
// }

func (s Stripe) handleWebhook(w http.ResponseWriter, r *http.Request) {
	// Immediately acknowledge receipt to Stripe
	w.WriteHeader(http.StatusOK)

	var event stripe.Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		s.app.logger.Error("Failed to decode Stripe event", "error", err)
		return
	}

	objectName := string(event.Type)
	// If the event type contains a period, we only want the part before it
	if idx := indexOfPeriod(objectName); idx > 0 {
		objectName = objectName[:idx]
	}

	if _, ok := s.app.modelMap[objectName]; !ok {
		s.app.logger.Error("No model found for event type", "event_type", objectName)
		return
	}

	if model, ok := s.app.modelMap[objectName]; ok {

		var obj map[string]interface{}
		err := json.Unmarshal(event.Data.Raw, &obj)
		if err != nil {
			s.app.logger.Error("Failed to unmarshal Stripe event data", "error", err)
			return
		}

		obj, err = s.mapProperties(obj)
		if err != nil {
			fmt.Printf("Failed to map properties for '%s': %v\n", objectName, err)
			return
		}

		err = model.Schema.Validate(obj)
		if err != nil {
			fmt.Printf("Object failed schema validation for '%s': %v\n", objectName, err)
			return
		}

		err = model.Queue.Enqueue(obj)
		if err != nil {
			fmt.Printf("Failed to enqueue object for '%s': %v\n", objectName, err)
			return
		}

	} else {
		fmt.Printf("No schema found for object: %s\n", objectName)
		return
	}
}

// indexOfPeriod returns the index of the first period in s, or -1 if not found
func indexOfPeriod(s string) int {
	for i, c := range s {
		if c == '.' {
			return i
		}
	}
	return -1
}

func (s Stripe) mapProperties(obj map[string]interface{}) (map[string]interface{}, error) {
	return mapProperties(obj, s.propertyMap)
}
