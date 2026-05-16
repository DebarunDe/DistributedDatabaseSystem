package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	pb "github.com/your-username/DistributedDatabaseSystem/proto/db"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:5555", "server address")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close connection: %v\n", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := pb.NewSQLServiceClient(conn)
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("db> ")
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "input error: %v\n", err)
			}
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if lower := strings.ToLower(line); lower == "exit" || lower == "quit" {
			break
		}

		resp, err := client.Execute(ctx, &pb.SQLRequest{Sql: line})
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			continue
		}

		if len(resp.Columns) > 0 {
			fmt.Println(strings.Join(resp.Columns, "\t"))
			fmt.Println(strings.Repeat("-", 40))
			for _, row := range resp.Rows {
				parts := make([]string, len(row.Fields))
				for i, f := range row.Fields {
					switch v := f.Value.(type) {
					case *pb.FieldValue_IntValue:
						parts[i] = fmt.Sprintf("%d", v.IntValue)
					case *pb.FieldValue_StringValue:
						parts[i] = v.StringValue
					case *pb.FieldValue_BoolValue:
						parts[i] = fmt.Sprintf("%t", v.BoolValue)
					default:
						parts[i] = "NULL"
					}
				}
				fmt.Println(strings.Join(parts, "\t"))
			}
		} else {
			fmt.Println("OK")
		}
	}
}
