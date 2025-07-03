package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/stripe/stripe-go/v82"

	"github.com/joncrlsn/dque"
)

var receiverWebhookCh = make(chan struct{})

type Stripe struct {
	Client *stripe.Client
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
	}

	app.receiveHandlers[systemInfo.Route] = app.stripeWebhookHealthcheck

	if systemInfo.UseCliListener {

		if _, err := exec.LookPath("stripe"); err != nil {
			return nil, fmt.Errorf("Stripe CLI not found in PATH. Please install it to use Stripe listening mode: %w", err)
		}

		// Forward Stripe events to our local endpoint
		forwardURL := fmt.Sprintf("http://localhost:%d%v", app.config.port, systemInfo.Route)
		cmd := exec.Command("stripe", "listen", "--forward-to", forwardURL)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Stdin = os.Stdin

		cmd.Env = append(os.Environ(), fmt.Sprintf("STRIPE_API_KEY=%s", systemInfo.ApiKey))

		go func() {
			fmt.Println("Starting Stripe CLI listener")
			err := cmd.Run()
			if err != nil {
				return
			}
		}()

	}
	stripeCmd := exec.Command("stripe", "trigger", "coupon.created")
	stripeCmd.Stdout = os.Stdout
	stripeCmd.Stderr = os.Stderr
	stripeCmd.Env = append(os.Environ(), fmt.Sprintf("STRIPE_API_KEY=%s", systemInfo.ApiKey))
	err = stripeCmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run stripe trigger: %w", err)
	}

	// Block until we receive something on receiverWebhookCh or timeout after 5 seconds
	select {
	case <-receiverWebhookCh:
		// Received signal, proceed with Stripe system initialization
		fmt.Println("Proceeding with Stripe system initialization.")
	case <-time.After(5 * time.Second):
		// Timeout after 5 seconds
		return nil, fmt.Errorf("timeout waiting for Stripe webhook healthcheck")
	}

	// Register the Stripe webhook handler
	app.receiveHandlers[systemInfo.Route] = app.stripeHandler

	stripeSystem := &Stripe{
		Client: stripeClient,
	}

	return stripeSystem, nil
}

func (app *application) stripeWebhookHealthcheck(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Stripe healthcheck received")
	_, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}
	select {
	case receiverWebhookCh <- struct{}{}:
	default:
	}
	w.WriteHeader(http.StatusOK)
}

func (app *application) stripeHandler(w http.ResponseWriter, r *http.Request) {
	// Immediately acknowledge receipt to Stripe
	w.WriteHeader(http.StatusOK)

	// This is a placeholder for the actual Stripe webhook handler
	fmt.Println("Stripe webhook received on route:", r.URL.Path)

	var event stripe.Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		fmt.Println("failed to parse webhook event:", err)
		return
	}
	fmt.Printf("Parsed Stripe event: ID=%s, Type=%s\n", event.ID, event.Type)
	fmt.Printf("Event payload: %s\n", string(event.Data.Raw))

	// Check for queue by top-level object name (before the first period)
	if app.queueMap != nil {
		eventTypeStr := string(event.Type)
		objectName := eventTypeStr
		if idx := indexOfPeriod(eventTypeStr); idx > 0 {
			objectName = eventTypeStr[:idx]
		}
		if _, ok := app.queueMap[objectName]; ok {
			fmt.Printf("Queue exists for object: %s\n", objectName)
		} else {
			fmt.Printf("No queue found for object: %s\n", objectName)
		}
	} else {
		fmt.Println("app.queueMap is nil")
	}

	// Validate event object against schema if available
	if app.compiledSchemas != nil {
		eventTypeStr := string(event.Type)
		objectName := eventTypeStr
		if idx := indexOfPeriod(eventTypeStr); idx > 0 {
			objectName = eventTypeStr[:idx]
		}
		if schema, ok := app.compiledSchemas[objectName]; ok {
			// Unmarshal event.Data.Raw into a generic map
			var obj map[string]interface{}
			if err := json.Unmarshal(event.Data.Raw, &obj); err != nil {
				fmt.Printf("Failed to unmarshal event.Data.Raw for schema validation: %v\n", err)
			} else {
				if err := schema.Validate(obj); err != nil {
					fmt.Printf("Object failed schema validation for '%s': %v\n", objectName, err)
				} else {
					fmt.Printf("Object validated successfully against schema '%s'\n", objectName)
					if queue, ok := app.queueMap[objectName]; ok {
						// Enqueue the object
						if err := queue.Enqueue(obj); err != nil {
							fmt.Printf("Failed to enqueue object for '%s': %v\n", objectName, err)
						}
						fmt.Printf("Object enqueued for '%s'\n", objectName)
					} else {
						fmt.Printf("Queue not found for object: %s\n", objectName)
					}
				}
			}
		} else {
			fmt.Printf("No schema found for object: %s\n", objectName)
		}
	} else {
		fmt.Println("app.compiledSchemas is nil")
	}

	// Print all items in the queue for the "price" object, if it exists
	if app.queueMap != nil {
		if queue, ok := app.queueMap["price"]; ok {
			iface, err := queue.Dequeue()
			if err != nil {
				if err != dque.ErrEmpty {
					log.Fatal("Error dequeuing item ", err)
				}
			}

			fmt.Println("Dequeued item from 'price' queue:", iface)

			if err != nil {
				// handle error
			}
		} else {
			fmt.Println("No queue found for 'price' object")
		}
	} else {
		fmt.Println("app.queueMap is nil")
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
