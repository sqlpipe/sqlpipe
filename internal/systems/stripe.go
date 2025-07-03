package systems

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/stripe/stripe-go/v82"
)

var receiverWebhookCh = make(chan struct{})

type Stripe struct {
	Client *stripe.Client
}

func newStripe(systemInfo SystemInfo, port int, receiveHandlers *map[string]func(http.ResponseWriter, *http.Request)) (system System, err error) {

	stripeClient := stripe.NewClient(systemInfo.ApiKey)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	listParams := &stripe.CustomerListParams{}
	listParams.Limit = stripe.Int64(1)

	for _, err := range stripeClient.V1Customers.List(ctx, listParams) {
		// We are only testing the connection, so we don't want to do anything with
		// the customer data. Do not read it, store it, print it, or log it. Nothing!
		if err != nil {
			return nil, err
		}
	}

	if systemInfo.UseCliListener {

		if _, err := exec.LookPath("stripe"); err != nil {
			return nil, fmt.Errorf("Stripe CLI not found in PATH. Please install it to use Stripe listening mode: %w", err)
		}

		(*receiveHandlers)[systemInfo.Route] = stripeWebhookHealthcheck

		// Forward Stripe events to our local endpoint
		forwardURL := fmt.Sprintf("http://localhost:%d%v", port, systemInfo.Route)
		cmd := exec.Command("stripe", "listen", "--forward-to", forwardURL)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Stdin = os.Stdin

		// Set the STRIPE_API_KEY environment variable from systemInfo
		cmd.Env = append(os.Environ(), fmt.Sprintf("STRIPE_API_KEY=%s", systemInfo.ApiKey))

		go func() {
			fmt.Println("Starting Stripe CLI listener")
			err := cmd.Run()
			if err != nil {
				return
			}
		}()

		stripeCmd := exec.Command("stripe", "trigger", "payment_intent.succeeded")
		// stripeCmd.Stdout = os.Stdout
		// stripeCmd.Stderr = os.Stderr
		stripeCmd.Env = append(os.Environ(), fmt.Sprintf("STRIPE_API_KEY=%s", systemInfo.ApiKey))
		err := stripeCmd.Run()
		if err != nil {
			return nil, fmt.Errorf("failed to run stripe trigger: %w", err)
		}
	}

	// Block until we receive something on receiverWebhookCh or timeout after 5 seconds
	select {
	case <-receiverWebhookCh:
		// Received signal, proceed with Stripe system initialization
		fmt.Println("Stripe webhook healthcheck received, proceeding with Stripe system initialization.")
	case <-time.After(5 * time.Second):
		// Timeout after 5 seconds
		return nil, fmt.Errorf("timeout waiting for Stripe webhook healthcheck")
	}

	stripeSystem := &Stripe{
		Client: stripeClient,
	}

	return stripeSystem, nil
}

func stripeWebhookHealthcheck(w http.ResponseWriter, r *http.Request) {
	fmt.Println("YOOOOO")
	route := r.URL.Path
	_, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}
	fmt.Printf("Received Stripe webhook on route: %s\n", route)
	select {
	case receiverWebhookCh <- struct{}{}:
	default:
	}
	w.WriteHeader(http.StatusOK)
}
