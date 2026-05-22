package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
	pb "github.com/your-username/DistributedDatabaseSystem/proto/repl"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// fatalErr wraps an error that should not trigger a reconnect retry.
type fatalErr struct{ err error }

func (f *fatalErr) Error() string { return f.err.Error() }
func (f *fatalErr) Unwrap() error { return f.err }

type ReplicationClient struct {
	bt             *btree.BTree
	pm             pagemanager.PageManager
	lastAppliedLSN uint64
	leaderAddr     string
	onBatch        func() error
}

func NewReplicationClient(bt *btree.BTree, pm pagemanager.PageManager, lastAppliedLSN uint64, leaderAddr string, onBatch func() error) *ReplicationClient {
	return &ReplicationClient{
		bt:             bt,
		pm:             pm,
		lastAppliedLSN: lastAppliedLSN,
		leaderAddr:     leaderAddr,
		onBatch:        onBatch,
	}
}

func protoToField(fv *pb.FieldValue, tag uint8) btree.Field {
	switch v := fv.Value.(type) {
	case *pb.FieldValue_IntValue:
		return btree.Field{Tag: tag, Value: btree.IntValue{V: v.IntValue}}
	case *pb.FieldValue_StringValue:
		return btree.Field{Tag: tag, Value: btree.StringValue{V: v.StringValue}}
	case *pb.FieldValue_BoolValue:
		s := "FALSE"
		if v.BoolValue {
			s = "TRUE"
		}
		return btree.Field{Tag: tag, Value: btree.StringValue{V: s}}
	case *pb.FieldValue_ListValue:
		elems := make([]btree.Value, len(v.ListValue.Elems))
		for i, e := range v.ListValue.Elems {
			elems[i] = protoToField(e, 0).Value
		}
		return btree.Field{Tag: tag, Value: btree.ListValue{
			ElemType: uint8(v.ListValue.ElemType),
			Elems:    elems,
		}}
	default:
		return btree.Field{Tag: tag, Value: btree.NullValue{}}
	}
}

func (rc *ReplicationClient) Start(ctx context.Context) error {
	const maxBackoff = 30 * time.Second
	backoff := time.Second

	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := rc.runOnce(ctx); err != nil {
			var fe *fatalErr
			if errors.As(err, &fe) {
				return fe.err
			}
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("replication client: %v; reconnecting in %v", err, backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil
			}
			if backoff < maxBackoff/2 {
				backoff *= 2
			} else {
				backoff = maxBackoff
			}
		}
	}
}

func (rc *ReplicationClient) runOnce(ctx context.Context) error {
	conn, err := grpc.NewClient(rc.leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", rc.leaderAddr, err)
	}
	defer func() { _ = conn.Close() }()

	stream, err := pb.NewReplicationServiceClient(conn).StreamUpdates(ctx, &pb.PullRequest{StartLsn: rc.lastAppliedLSN})
	if err != nil {
		return fmt.Errorf("stream updates: %w", err)
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("recv: %w", err)
			}
			// Explicit gRPC status error (server-side rejection) with a non-transient
			// code should not be retried — propagate immediately.
			if s, ok := status.FromError(err); ok {
				switch s.Code() {
				case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
					// transient — let the retry loop reconnect
				default:
					return &fatalErr{fmt.Errorf("recv: %w", err)}
				}
			}
			return fmt.Errorf("recv: %w", err)
		}

		for _, entry := range resp.Entries {
			fields := make([]btree.Field, len(entry.Fields))
			for i, fv := range entry.Fields {
				fields[i] = protoToField(fv, uint8(i+1))
			}
			switch ReplOp(entry.Op) {
			case ReplPut:
				if err := rc.bt.Insert(entry.Key, fields); err != nil {
					return fmt.Errorf("apply insert (lsn=%d): %w", entry.Lsn, err)
				}
			case ReplDelete:
				if err := rc.bt.Delete(entry.Key); err != nil {
					return fmt.Errorf("apply delete (lsn=%d): %w", entry.Lsn, err)
				}
			}
			rc.lastAppliedLSN = entry.Lsn + 1
		}

		if rc.onBatch != nil {
			if err := rc.onBatch(); err != nil {
				return &fatalErr{fmt.Errorf("post-batch: %w", err)}
			}
		}

		if err := rc.pm.SetMetaCheckpointLSN(rc.lastAppliedLSN); err != nil {
			return &fatalErr{fmt.Errorf("checkpoint lsn=%d: %w", rc.lastAppliedLSN, err)}
		}
	}
}
