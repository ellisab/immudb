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
package client

import (
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/protobuf/types/known/emptypb"
)

const SQLPrefix byte = 2

func (c *immuClient) SQLExec(ctx context.Context, sql string, params map[string]interface{}) (*schema.SQLExecResult, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	namedParams, err := encodeParams(params)
	if err != nil {
		return nil, err
	}

	return c.ServiceClient.SQLExec(ctx, &schema.SQLExecRequest{Sql: sql, Params: namedParams})
}

func (c *immuClient) UseSnapshot(ctx context.Context, sinceTx, asBeforeTx uint64) error {
	if !c.IsConnected() {
		return ErrNotConnected
	}

	_, err := c.ServiceClient.UseSnapshot(ctx, &schema.UseSnapshotRequest{SinceTx: sinceTx, AsBeforeTx: asBeforeTx})
	return err
}

func (c *immuClient) SQLQuery(ctx context.Context, sql string, params map[string]interface{}, renewSnapshot bool) (*schema.SQLQueryResult, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	namedParams, err := encodeParams(params)
	if err != nil {
		return nil, err
	}

	return c.ServiceClient.SQLQuery(ctx, &schema.SQLQueryRequest{Sql: sql, Params: namedParams, ReuseSnapshot: !renewSnapshot})
}

func (c *immuClient) ListTables(ctx context.Context) (*schema.SQLQueryResult, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.ListTables(ctx, &emptypb.Empty{})
}

func (c *immuClient) DescribeTable(ctx context.Context, tableName string) (*schema.SQLQueryResult, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.DescribeTable(ctx, &schema.Table{TableName: tableName})
}

func encodeParams(params map[string]interface{}) ([]*schema.NamedParam, error) {
	if params == nil {
		return nil, nil
	}

	namedParams := make([]*schema.NamedParam, len(params))

	i := 0
	for n, v := range params {
		sqlVal, err := asSQLValue(v)
		if err != nil {
			return nil, err
		}

		namedParams[i] = &schema.NamedParam{Name: n, Value: sqlVal}
		i++
	}

	return namedParams, nil
}

func (c *immuClient) VerifyRow(ctx context.Context, row *schema.Row, table string, pkVal *schema.SQLValue) error {
	if row == nil || len(table) == 0 || pkVal == nil {
		return ErrIllegalArguments
	}

	if len(row.Columns) == 0 || len(row.Columns) != len(row.Values) {
		return sql.ErrCorruptedData
	}

	if !c.IsConnected() {
		return ErrNotConnected
	}

	err := c.StateService.CacheLock()
	if err != nil {
		return err
	}
	defer c.StateService.CacheUnlock()

	state, err := c.StateService.GetState(ctx, c.currentDatabase())
	if err != nil {
		return err
	}

	vEntry, err := c.ServiceClient.VerifiableSQLGet(ctx, &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{Table: table, PkValue: pkVal},
		ProveSinceTx:  state.TxId,
	})
	if err != nil {
		return err
	}

	inclusionProof := schema.InclusionProofFrom(vEntry.InclusionProof)
	dualProof := schema.DualProofFrom(vEntry.VerifiableTx.DualProof)

	var eh [sha256.Size]byte

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	vTx := vEntry.SqlEntry.Tx

	dbID := vEntry.DatabaseId
	tableID := vEntry.TableId
	pkID, ok := vEntry.ColIdsByName[sql.EncodeSelector("", c.currentDatabase(), table, vEntry.PKName)]
	if !ok {
		return sql.ErrCorruptedData
	}
	pkType, ok := vEntry.ColTypesById[pkID]
	if !ok {
		return sql.ErrCorruptedData
	}

	pkEncVal, err := sql.EncodeRawValue(schema.RawValue(pkVal), pkType, true)
	if err != nil {
		return err
	}

	pkKey := sql.MapKey([]byte{SQLPrefix}, sql.RowPrefix, sql.EncodeID(dbID), sql.EncodeID(tableID), sql.EncodeID(pkID), pkEncVal)

	decodedRow, err := decodeRow(vEntry.SqlEntry.Value, vEntry.ColTypesById)
	if err != nil {
		return err
	}

	err = verifyRowAgainst(row, decodedRow, vEntry.ColIdsByName)
	if err != nil {
		return err
	}

	kv := &store.KV{Key: pkKey, Value: vEntry.SqlEntry.Value}

	if state.TxId <= vTx {
		eh = schema.DigestFrom(vEntry.VerifiableTx.DualProof.TargetTxMetadata.EH)

		sourceID = state.TxId
		sourceAlh = schema.DigestFrom(state.TxHash)
		targetID = vTx
		targetAlh = dualProof.TargetTxMetadata.Alh()
	} else {
		eh = schema.DigestFrom(vEntry.VerifiableTx.DualProof.SourceTxMetadata.EH)

		sourceID = vTx
		sourceAlh = dualProof.SourceTxMetadata.Alh()
		targetID = state.TxId
		targetAlh = schema.DigestFrom(state.TxHash)
	}

	verifies := store.VerifyInclusion(
		inclusionProof,
		kv,
		eh)
	if !verifies {
		return store.ErrCorruptedData
	}

	if state.TxId > 0 {
		verifies = store.VerifyDualProof(
			dualProof,
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if !verifies {
			return store.ErrCorruptedData
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: vEntry.VerifiableTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		ok, err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return err
		}
		if !ok {
			return store.ErrCorruptedData
		}
	}

	err = c.StateService.SetState(c.currentDatabase(), newState)
	if err != nil {
		return err
	}

	return nil
}

func verifyRowAgainst(row *schema.Row, decodedRow map[uint64]*schema.SQLValue, colIdsByName map[string]uint64) error {
	for i, colName := range row.Columns {
		colID, ok := colIdsByName[colName]
		if !ok {
			return sql.ErrColumnDoesNotExist
		}

		val := row.Values[i]

		if val == nil || val.Value == nil {
			return sql.ErrCorruptedData
		}

		decodedVal, ok := decodedRow[colID]
		if !ok {
			_, isNull := val.Value.(*schema.SQLValue_Null)
			if isNull {
				continue
			}
			return sql.ErrCorruptedData
		}

		if decodedVal == nil || decodedVal.Value == nil {
			return sql.ErrCorruptedData
		}

		equals, err := val.Value.(schema.SqlValue).Equal(decodedVal.Value.(schema.SqlValue))
		if err != nil {
			return err
		}
		if !equals {
			return sql.ErrCorruptedData
		}
	}

	return nil
}

func decodeRow(encodedRow []byte, colTypes map[uint64]sql.SQLValueType) (map[uint64]*schema.SQLValue, error) {
	off := 0

	if len(encodedRow) < off+sql.EncLenLen {
		return nil, sql.ErrCorruptedData
	}

	colsCount := binary.BigEndian.Uint32(encodedRow[off:])
	off += sql.EncLenLen

	values := make(map[uint64]*schema.SQLValue, colsCount)

	for i := 0; i < int(colsCount); i++ {
		if len(encodedRow) < off+sql.EncIDLen {
			return nil, sql.ErrCorruptedData
		}

		colID := binary.BigEndian.Uint64(encodedRow[off:])
		off += sql.EncIDLen

		colType, ok := colTypes[colID]
		if !ok {
			return nil, sql.ErrCorruptedData
		}

		val, n, err := sql.DecodeValue(encodedRow[off:], colType)
		if err != nil {
			return nil, err
		}

		values[colID] = typedValueToRowValue(val)
		off += n
	}

	return values, nil
}

func asSQLValue(v interface{}) (*schema.SQLValue, error) {
	if v == nil {
		return &schema.SQLValue{Value: &schema.SQLValue_Null{}}, nil
	}

	switch tv := v.(type) {
	case uint:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}, nil
		}
	case int:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}, nil
		}
	case int64:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}, nil
		}
	case uint64:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}, nil
		}
	case string:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_S{S: tv}}, nil
		}
	case bool:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_B{B: tv}}, nil
		}
	case []byte:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_Bs{Bs: tv}}, nil
		}
	}

	return nil, sql.ErrInvalidValue
}

func typedValueToRowValue(tv sql.TypedValue) *schema.SQLValue {
	switch tv.Type() {
	case sql.IntegerType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: tv.Value().(uint64)}}
		}
	case sql.VarcharType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_S{S: tv.Value().(string)}}
		}
	case sql.BooleanType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_B{B: tv.Value().(bool)}}
		}
	case sql.BLOBType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_Bs{Bs: tv.Value().([]byte)}}
		}
	}
	return nil
}
