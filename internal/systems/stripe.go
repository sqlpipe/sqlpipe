package systems

import (
	"context"
	"time"

	"github.com/stripe/stripe-go/v82"
)

type Stripe struct {
	Client *stripe.Client
}

func newStripe(systemInfo SystemInfo) (system System, err error) {

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

	stripeSystem := &Stripe{
		Client: stripeClient,
	}

	return stripeSystem, nil
}
