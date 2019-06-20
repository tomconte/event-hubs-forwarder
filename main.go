package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"
	"sync/atomic"

	"github.com/Azure/azure-amqp-common-go/v2/conn"
	"github.com/Azure/azure-amqp-common-go/v2/sas"
	"github.com/Azure/azure-event-hubs-go/v2"
	"github.com/Azure/azure-event-hubs-go/v2/eph"
	"github.com/Azure/azure-event-hubs-go/v2/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
)

var counter uint64

func main() {
	// Azure Storage account information
	storageAccountName := os.Getenv("EPH_STORAGE_ACCOUNT")
	storageAccountKey := os.Getenv("EPH_ACCOUNT_KEY")

	// Azure Storage container to store leases and checkpoints
	storageContainerName := os.Getenv("EPH_STORAGE_CONTAINER")

	// Azure Event Hubs connection string
	eventHubConnStr := os.Getenv("SOURCE_EVENTHUB_CS")
	parsed, err := conn.ParsedConnectionFromStr(eventHubConnStr)
	if err != nil {
		// handle error
	}

	// Create a new Azure Storage Leaser/Checkpointer
	cred, err := azblob.NewSharedKeyCredential(storageAccountName, storageAccountKey)
	if err != nil {
		fmt.Println(err)
		return
	}

	leaserCheckpointer, err := storage.NewStorageLeaserCheckpointer(cred, storageAccountName, storageContainerName, azure.PublicCloud)
	if err != nil {
		fmt.Println(err)
		return
	}

	// SAS token provider for Azure Event Hubs
	provider, err := sas.NewTokenProvider(sas.TokenProviderWithKey(parsed.KeyName, parsed.Key))
	if err != nil {
		fmt.Println(err)
		return
	}

    // Create a context
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Create a new EPH processor
	processor, err := eph.New(ctx, parsed.Namespace, parsed.HubName, provider, leaserCheckpointer, leaserCheckpointer)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Register a message handler -- many can be registered
	handlerID, err := processor.RegisterHandler(ctx, eventHandler)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("handler id: %q is running\n", handlerID)

	// unregister a handler to stop that handler from receiving events
	// processor.UnregisterHandler(ctx, handleID)

	// start handling messages from all of the partitions balancing across multiple consumers
	err = processor.StartNonBlocking(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Set up ticker
	ticker := time.NewTicker(5 * time.Minute)
	go eventCounter(ticker)

	// Wait for a signal to quit
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan
}

/*
** Handle incoming events.
*/

func eventHandler(c context.Context, e *eventhub.Event) error {
	// Destination Event Hubs for forwarded events
	destEventHubConnStr := os.Getenv("DESTINATION_EVENTHUB_CS")

	hub, err := eventhub.NewHubFromConnectionString(destEventHubConnStr)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	err = hub.Send(c, e)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	err = hub.Close(c)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	
	atomic.AddUint64(&counter, 1)

	return nil
}

/*
** Log counts of forwarded events.
*/

func eventCounter(t *time.Ticker) {
	for tick := range t.C {
		curCounter := atomic.LoadUint64(&counter)
		fmt.Println(curCounter, "events forwarded at", tick)
	}
}
