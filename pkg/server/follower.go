/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package server

import (
	"context"
	"strings"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc/metadata"
)

func (s *ImmuServer) IsMaster() bool {
	return s.master == nil
}

func (s *ImmuServer) IsFollower() bool {
	return s.master != nil
}

func (s *ImmuServer) followerLogin() (client.ImmuClient, context.Context, error) {
	opts := client.DefaultOptions().WithAddress(s.master.address).WithPort(s.master.port)

	ctx := context.Background()

	client, err := client.NewImmuClient(opts)
	if err != nil {
		s.Logger.Errorf("Failed to connect. Reason: %v", err)
		return nil, ctx, err
	}

	login, err := client.Login(ctx, []byte(s.followerUser), []byte(s.followerPwd))
	if err != nil {
		s.Logger.Errorf("Failed to login. Reason: %v", err)
		return nil, ctx, err
	}

	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", login.GetToken()))

	return client, ctx, nil
}

func (s *ImmuServer) follow() {
	s.Logger.Infof("Following master %s:%d", s.master.address, s.master.port)
	defer func() {
		s.Logger.Infof("Replication stopped.")
	}()

	client, ctx, err := s.followerLogin()
	if err != nil {
		s.Logger.Errorf("Failed to login. Reason %s %s: %v", s.followerUser, s.followerPwd, err)
	}
	defer client.Logout(ctx)

	for {
		select {
		case <-time.Tick(1 * time.Millisecond):
			{
				db := s.dbList.GetByIndex(s.databasenameToIndex[s.Options.defaultDbName])

				state, err := db.CurrentState()
				if err != nil {
					s.Logger.Warningf("Replication got error: %v", err)
					return
				}

				tx, err := client.TxByID(ctx, state.TxId+1)
				if err != nil && strings.Contains(err.Error(), "tx not found") {
					s.Logger.Infof("Follower up to date with %s:%d!", s.master.address, s.master.port)
					time.Sleep(10 * time.Second)
					continue
				}
				if err != nil {
					s.Logger.Warningf("Replication got error when trying to fetch tx from master: %v", err)

					time.Sleep(5 * time.Second)

					client, ctx, err = s.followerLogin()
					if err != nil {
						s.Logger.Errorf("Failed to login. Reason: %v", err)
						continue
					}

					continue
				}

				kvs := make([]*schema.KeyValue, len(tx.Entries))

				for i, kv := range tx.Entries {
					e, err := client.GetAt(ctx, kv.Key, tx.Metadata.Id)
					if err != nil {
						s.Logger.Warningf("Replication got error when trying to fetch tx data from master: %v", err)

						time.Sleep(5 * time.Second)

						client, ctx, err = s.followerLogin()
						if err != nil {
							s.Logger.Errorf("Failed to login. Reason: %v", err)
							continue
						}

						return
					}

					kvs[i] = &schema.KeyValue{Key: kv.Key, Value: e.Value}
				}

				_, err = db.ReplicatedSet(kvs, true, tx.Metadata.Ts, tx.Metadata.BlTxId)
				if err != nil {
					s.Logger.Warningf("Replication got error: %v", err)
				}
			}
		case <-s.quit:
			{
				return
			}
		}
	}
}
