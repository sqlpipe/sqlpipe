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
	client          *stripe.Client
	app             *application
	receiveFieldMap map[string]map[string]map[string]Location
	pushFieldMap    map[string]map[string]map[string]Location
	systemInfo      SystemInfo
}

func (app *application) newStripe(systemInfo SystemInfo) (system SystemInterface, err error) {

	if systemInfo.UseCliListener {

		if _, err := exec.LookPath("stripe"); err != nil {
			return nil, fmt.Errorf("Stripe CLI not found in PATH. Please install it to use Stripe listening mode: %w", err)
		}

		// Forward Stripe events to our local endpoint
		forwardURL := fmt.Sprintf("http://localhost:%d/%v", app.config.port, systemInfo.Name)
		cmd := exec.Command("stripe", "listen", "--forward-to", forwardURL)
		cmd.Stderr = os.Stderr

		cmd.Env = append(os.Environ(), fmt.Sprintf("STRIPE_API_KEY=%s", systemInfo.ApiKey))

		go func() {
			app.logger.Info("Starting Stripe CLI listener", "command", cmd.String())
			err := cmd.Run()
			if err != nil {
				return
			}
		}()
	}

	stripeClient := stripe.NewClient(systemInfo.ApiKey)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	listParams := &stripe.CouponListParams{}
	listParams.Limit = stripe.Int64(1)

	for _, err := range stripeClient.V1Coupons.List(ctx, listParams) {
		// We are only testing the connection, so we don't want to do anything with
		// the data. Do not read it, store it, print it, or log it. Nothing!
		if err != nil {
			return nil, err
		}

		break
	}

	app.logger.Info("Stripe test api call was successful", "system", systemInfo.Name)

	stripeSystem := &Stripe{
		client:          stripeClient,
		app:             app,
		receiveFieldMap: app.receiveFieldMap[systemInfo.Name],
		pushFieldMap:    app.pushFieldMap[systemInfo.Name],
		systemInfo:      systemInfo,
	}

	return stripeSystem, nil
}

func (s *Stripe) handleWebhook(w http.ResponseWriter, r *http.Request) {
	// Immediately acknowledge receipt to Stripe
	w.WriteHeader(http.StatusOK)

	fmt.Println("Received Stripe webhook")

	var event stripe.Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		s.app.logger.Error("Failed to decode Stripe event", "error", err)
		return
	}

	s.app.logger.Info("Received Stripe webhook", "type", event.Type)

	objectName := string(event.Type)
	// If the event type contains a period, we only want the part before it
	if idx := indexOfPeriod(objectName); idx > 0 {
		objectName = objectName[:idx]
	}

	var obj map[string]interface{}
	err := json.Unmarshal(event.Data.Raw, &obj)
	if err != nil {
		s.app.logger.Error("Failed to unmarshal Stripe event data", "error", err)
		return
	}

	newObjs := make(map[string]map[string]interface{})

	for schemaName, fieldMap := range s.receiveFieldMap[objectName] {
		newModel := map[string]interface{}{}

		for keyInObj, desiredKey := range fieldMap {
			if desiredKey.Pull {
				newModel[desiredKey.Field] = obj[keyInObj]
			}
		}

		newObjs[schemaName] = newModel
	}

	for schemaName, obj := range newObjs {

		schema, inMap := s.app.schemaMap[schemaName]
		if !inMap {
			fmt.Printf("No schema found for object: %s\n", objectName)
			return
		}

		err = schema.Validate(obj)
		if err != nil {
			fmt.Printf("Object failed schema validation for '%s': %v\n", objectName, err)
			return
		}

		s.app.storageEngine.addSafeObject(obj, schemaName)
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
