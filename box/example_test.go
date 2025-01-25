// Run Tarantool Common Edition before example execution:
//
// Terminal 1:
// $ cd box
// $ TEST_TNT_LISTEN=127.0.0.1:3013 tarantool testdata/config.lua
//
// Terminal 2:
// $ go test -v example_test.go
package box_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/box"
)

func ExampleBox_Info() {
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	client, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	cancel()
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	// You can use Info Request type.

	fut := client.Do(box.NewInfoRequest())

	resp := &box.InfoResponse{}

	err = fut.GetTyped(resp)
	if err != nil {
		log.Fatalf("Failed get box info: %s", err)
	}

	// Or use simple Box implementation.

	b := box.New(client)

	info, err := b.Info()
	if err != nil {
		log.Fatalf("Failed get box info: %s", err)
	}

	if info.UUID != resp.Info.UUID {
		log.Fatalf("Box info uuids are not equal")
	}

	fmt.Printf("Box info uuids are equal")
	fmt.Printf("Current box info: %+v\n", resp.Info)
}

func ExampleSchemaUser_Exists() {
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	client, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	cancel()
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	// You can use UserExistsRequest type and call it directly.
	fut := client.Do(box.NewUserExistsRequest("user"))

	resp := &box.UserExistsResponse{}

	err = fut.GetTyped(resp)
	if err != nil {
		log.Fatalf("Failed get box schema user exists with error: %s", err)
	}

	// Or use simple User implementation.
	b := box.New(client)
	exists, err := b.Schema().User().Exists(ctx, "user")
	if err != nil {
		log.Fatalf("Failed get box schema user exists with error: %s", err)
	}

	if exists != resp.Exists {
		log.Fatalf("Box schema users exists are not equal")
	}

	fmt.Printf("Box schema users exists are equal")
	fmt.Printf("Current exists state: %+v\n", exists)
}

func ExampleSchemaUser_Create() {
	// Connect to Tarantool.
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	client, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	cancel()
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	// Create SchemaUser.
	schemaUser := box.NewSchemaUser(client)

	// Create a new user.
	username := "new_user"
	options := box.UserCreateOptions{
		IfNotExists: true,
		Password:    "secure_password",
	}
	err = schemaUser.Create(ctx, username, options)
	if err != nil {
		log.Fatalf("Failed to create user: %s", err)
	}

	fmt.Printf("User '%s' created successfully\n", username)
}

func ExampleSchemaUser_Drop() {
	// Connect to Tarantool
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	client, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	cancel()
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	// Create SchemaUser
	schemaUser := box.NewSchemaUser(client)

	// Drop an existing user
	username := "new_user"
	options := box.UserDropOptions{
		IfExists: true,
	}
	err = schemaUser.Drop(ctx, username, options)
	if err != nil {
		log.Fatalf("Failed to drop user: %s", err)
	}

	fmt.Printf("User '%s' dropped successfully\n", username)
}

func ExampleSchemaUser_Password() {
	// Connect to Tarantool
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	client, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	cancel()
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	// Create SchemaUser
	schemaUser := box.NewSchemaUser(client)

	// Get the password hash
	password := "my-password"
	passwordHash, err := schemaUser.Password(ctx, password)
	if err != nil {
		log.Fatalf("Failed to get password hash: %s", err)
	}

	fmt.Printf("Password '%s' hash: %s\n", password, passwordHash)
}
