package systems

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/stripe/stripe-go/v82"
)

type Stripe struct {
	Client *stripe.Client
}

func newStripe(systemInfo SystemInfo, port int, router *httprouter.Router) (system System, err error) {

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

		// define a new route for the Stripe listener
		if router != nil {
			router.HandlerFunc(http.MethodPost, systemInfo.Route, handleStripeWebhook)
		}

		go func() {
			// Forward Stripe events to our local endpoint
			forwardURL := fmt.Sprintf("http://localhost:%d%v", port, systemInfo.Route)
			cmd := exec.Command("stripe", "listen", "--forward-to", forwardURL)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.Stdin = os.Stdin

			// Set the STRIPE_API_KEY environment variable from systemInfo
			cmd.Env = append(os.Environ(), fmt.Sprintf("STRIPE_API_KEY=%s", systemInfo.ApiKey))

			if err := cmd.Run(); err != nil {
				return
			}
		}()
	}

	stripeSystem := &Stripe{
		Client: stripeClient,
	}

	return stripeSystem, nil
}

func handleStripeWebhook(w http.ResponseWriter, r *http.Request) {
	route := r.URL.Path
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}
	fmt.Printf("Received Stripe webhook on route: %s\nPayload: %s\n", route, string(payload))
	w.WriteHeader(http.StatusOK)
}
