package carbium_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	carbium "github.com/carbium-sh/carbium-go"
)

// The README's quick start, as pkg.go.dev shows it. It calls the service over the network, so it has no Output line
// and is never run by go test; TestEveryGoBlockInTheReadmeVets compiles the README's own copy.
func Example() {
	c, err := carbium.NewClient(os.Getenv("CARBIUM_KEY"), nil)
	if err != nil {
		panic(err)
	}
	bh, err := c.GetLatestBlockhash(context.Background(), nil)
	var e *carbium.Error
	if errors.As(err, &e) {
		fmt.Println(e.Kind, e.HTTPStatus, e.Code, e.Reason, e.Message)
		return
	}
	fmt.Println("blockhash", bh.Value.Blockhash, "valid until block height", bh.Value.LastValidBlockHeight, "at slot", bh.Context.Slot)
}
