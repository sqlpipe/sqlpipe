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
	client            *stripe.Client
	app               *application
	systemPropertyMap map[string]map[string]map[string]string
	systemInfo        SystemInfo
}

func (app *application) newStripe(systemInfo SystemInfo) (system System, err error) {

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
			app.logger.Info("Starting Stripe CLI listener", "command", cmd.String())
			err := cmd.Run()
			if err != nil {
				return
			}
		}()
	}

	stripeSystem := &Stripe{
		client:            stripeClient,
		app:               app,
		systemPropertyMap: app.systemPropertyMap[systemInfo.Name],
		systemInfo:        systemInfo,
	}

	return stripeSystem, nil
}

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
		s.app.logger.Info("No model found for event type", "event_type", objectName)
		return
	}

	var obj map[string]interface{}
	err := json.Unmarshal(event.Data.Raw, &obj)
	if err != nil {
		s.app.logger.Error("Failed to unmarshal Stripe event data", "error", err)
		return
	}

	b, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		s.app.logger.Error("Failed to marshal object", "error", err)
		return
	}
	fmt.Println("obj:")
	fmt.Println(string(b))

	modelsToCreate := s.systemPropertyMap[objectName]

	b, err = json.MarshalIndent(modelsToCreate, "", "  ")
	if err != nil {
		s.app.logger.Error("Failed to marshal models to create", "error", err)
		return
	}
	fmt.Println("modelsToCreate:")
	fmt.Println(string(b))

	newModels := make(map[string]interface{})

	for modelName, fieldMap := range modelsToCreate {
		newModel := map[string]interface{}{}

		for keyInObj, desiredKey := range fieldMap {
			newModel[desiredKey] = obj[keyInObj]
		}

		newModels[modelName] = newModel
	}

	for modelName, model := range newModels {
		fmt.Println("new model:", modelName)
		b, err := json.MarshalIndent(model, "", "  ")
		if err != nil {
			s.app.logger.Error("Failed to marshal new model", "error", err)
			return
		}
		fmt.Println(string(b))
	}

	// if model, ok := s.app.modelMap[objectName]; ok {

	// 	var obj map[string]interface{}
	// 	err := json.Unmarshal(event.Data.Raw, &obj)
	// 	if err != nil {
	// 		s.app.logger.Error("Failed to unmarshal Stripe event data", "error", err)
	// 		return
	// 	}

	// 	obj, err = s.mapProperties(obj, modelName, incomingObjectType)
	// 	if err != nil {
	// 		fmt.Printf("Failed to map properties for '%s': %v\n", objectName, err)
	// 		return
	// 	}

	// 	err = model.Schema.Validate(obj)
	// 	if err != nil {
	// 		fmt.Printf("Object failed schema validation for '%s': %v\n", objectName, err)
	// 		return
	// 	}

	// 	err = model.Queue.Enqueue(obj)
	// 	if err != nil {
	// 		fmt.Printf("Failed to enqueue object for '%s': %v\n", objectName, err)
	// 		return
	// 	}

	// 	fmt.Printf("Enqueued object: %+v\n", obj)

	// } else {
	// 	fmt.Printf("No schema found for object: %s\n", objectName)
	// 	return
	// }
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
