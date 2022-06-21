package wire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/fatih/color"
	"github.com/jackc/pgproto3"
	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/internal/buffer"
	"github.com/jeroenrinzema/psql-wire/internal/types"
	"github.com/lib/pq/oid"
	"go.uber.org/zap"
)

// NewErrUnimplementedMessageType is called whenever a unimplemented message
// type is send. This error indicates to the client that the send message cannot
// be processed at this moment in time.
func NewErrUnimplementedMessageType(t types.ClientMessage) error {
	err := fmt.Errorf("unimplemented client message type: %d", t)
	return psqlerr.WithSeverity(psqlerr.WithCode(err, codes.ConnectionDoesNotExist), psqlerr.LevelFatal)
}

type SimpleQueryFn func(ctx context.Context, query string, writer DataWriter) error

type ParseFn func(ctx context.Context, query string, writer DataWriter) (PreparedStatement, error)

type CloseFn func(ctx context.Context) error

// consumeCommands consumes incoming commands send over the Postgres wire connection.
// Commands consumed from the connection are returned through a go channel.
// Responses for the given message type are written back to the client.
// This method keeps consuming messages until the client issues a close message
// or the connection is terminated.
func (srv *Server) consumeCommands(ctx context.Context, conn net.Conn, reader *buffer.Reader, writer *buffer.Writer) (err error) {
	srv.logger.Debug("ready for query... starting to consume commands")

	// TODO(Jeroen): include a indentification value inside the context that
	// could be used to identify connections at a later stage.

	for {
		err = readyForQuery(writer, types.ServerIdle)
		if err != nil {
			return err
		}

		t, length, err := reader.ReadTypedMsg()
		if err == io.EOF {
			return nil
		}

		// NOTE(Jeroen): we could recover from this scenario
		if errors.Is(err, buffer.ErrMessageSizeExceeded) {
			err = srv.handleMessageSizeExceeded(reader, writer, err)
			if err != nil {
				return err
			}

			continue
		}

		srv.logger.Debug("incoming command", zap.Int("length", length), zap.String("type", string(t)))

		if err != nil {
			return err
		}

		err = srv.handleCommand(ctx, conn, t, reader, writer)
		if err != nil {
			return err
		}
	}
}

// handleMessageSizeExceeded attempts to unwrap the given error message as
// message size exceeded. The expected message size will be consumed and
// discarded from the given reader. An error message is written to the client
// once the expected message size is read.
//
// The given error is returned if it does not contain an message size exceeded
// type. A fatal error is returned when an unexpected error is returned while
// consuming the expected message size or when attempting to write the error
// message back to the client.
func (srv *Server) handleMessageSizeExceeded(reader *buffer.Reader, writer *buffer.Writer, exceeded error) (err error) {
	unwrapped, has := buffer.UnwrapMessageSizeExceeded(exceeded)
	if !has {
		return exceeded
	}

	err = reader.Slurp(unwrapped.Size)
	if err != nil {
		return err
	}

	return ErrorCode(writer, exceeded)
}

// handleCommand handles the given client message. A client message includes a
// message type and reader buffer containing the actual message. The type
// indecates a action executed by the client.
func (srv *Server) handleCommand(ctx context.Context, conn net.Conn, t types.ClientMessage, reader *buffer.Reader, writer *buffer.Writer) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	switch t {
	case types.ClientSync:
		color.Red("ClientSync")
		// Send the sync completion status
		return srv.handleSync(ctx, reader, writer)
	case types.ClientSimpleQuery:
		color.Red("ClientSimpleQuery")
		return srv.handleSimpleQuery(ctx, reader, writer)
	case types.ClientExecute:
		color.Red("ClientExecute")
		// Run the portal and return final data
		return srv.handleExecute(ctx, reader, writer)
	case types.ClientParse:
		color.Red("ClientParse")
		// Run prepare statement
		return srv.handleParse(ctx, reader, writer)
	case types.ClientDescribe:
		color.Red("ClientDescribe")
		return srv.handleDescribe(ctx, reader, writer)
	case types.ClientBind:
		color.Red("ClientBind")
		// Query planning
		return srv.handleBind(ctx, reader, writer)
	case types.ClientFlush:
	case types.ClientCopyData, types.ClientCopyDone, types.ClientCopyFail:
		// We're supposed to ignore these messages, per the protocol spec. This
		// state will happen when an error occurs on the server-side during a copy
		// operation: the server will send an error and a ready message back to
		// the client, and must then ignore further copy messages. See:
		// https://github.com/postgres/postgres/blob/6e1dd2773eb60a6ab87b27b8d9391b756e904ac3/src/backend/tcop/postgres.c#L4295
		break
	case types.ClientClose:
		color.Red("ClientClose")
		err = srv.handleConnClose(ctx)
		if err != nil {
			return err
		}

		return conn.Close()
	case types.ClientTerminate:
		color.Red("ClientTerminate")
		err = srv.handleConnTerminate(ctx)
		if err != nil {
			return err
		}

		return conn.Close()
	default:
		srv.logger.Debug("INVALIDDDDDDDDDDDDDDDDDd")
		return ErrorCode(writer, NewErrUnimplementedMessageType(t))
	}

	return nil
}

func (srv *Server) handleSimpleQuery(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	if srv.SimpleQuery == nil {
		return ErrorCode(writer, NewErrUnimplementedMessageType(types.ClientSimpleQuery))
	}

	query, err := reader.GetString()
	if err != nil {
		return err
	}

	srv.logger.Debug("incoming query", zap.String("query", query))

	err = srv.SimpleQuery(ctx, query, &dataWriter{
		ctx:    ctx,
		client: writer,
	})

	if err != nil {
		return ErrorCode(writer, err)
	}

	return nil
}

func (srv *Server) handleSync(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	sync := pgproto3.Sync{}
	if err := sync.Decode(reader.Msg); err != nil {
		return err
	}
	fmt.Printf(color.YellowString("%+v\n"), sync)

	dw := &dataWriter{
		ctx:    ctx,
		client: writer,
	}

	return dw.Complete("Sync Complete")
}

func (srv *Server) handleExecute(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	// Run the portal for final data

	execMsg := pgproto3.Execute{}
	err := execMsg.Decode(reader.Msg)
	fmt.Printf(color.YellowString("%+v\n"), execMsg)
	return err
}

func (srv *Server) handleParse(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	parsed := pgproto3.Parse{}
	parsed.Decode(reader.Msg)
	fmt.Printf("\n\n%+v\n", parsed)

	srv.logger.Debug(color.YellowString("incoming parse statement"), zap.String("parse query", parsed.Query))
	if srv.ParseFn == nil {
		panic("Parse func is nil")
	}

	pstm, err := srv.ParseFn(ctx, parsed.Query, &dataWriter{
		ctx:    ctx,
		client: writer,
	})

	if err != nil {
		return ErrorCode(writer, err)
	}

	// Stored the parsed query
	srv.preparedStatements[parsed.Name] = pstm

	return nil
}

func (srv *Server) handleDescribe(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	describe := pgproto3.Describe{}
	describe.Decode(reader.Msg)
	fmt.Printf(color.YellowString("Describe Client input: %+v\n"), describe)

	srv.logger.Debug("incoming describe", zap.String("describe name", describe.Name))
	typ := types.PrepareType(describe.ObjectType)

	switch typ {
	case types.PrepareStatement:
		stmt, ok := srv.preparedStatements[describe.Name]
		if !ok {
			return fmt.Errorf("unknown prepared statement: %q", describe.Name)
		}
		fmt.Println(stmt)
		color.Blue("Describe type Prepare statement: %+v\n", stmt)

		dw := &dataWriter{
			ctx:    ctx,
			client: writer,
		}
		var table = Columns{
			{
				Table:  0,
				Name:   "age",
				Oid:    oid.T_int4,
				Width:  1,
				Format: TextFormat,
			},
			// {
			// Table:  0,
			// Name:   "name",
			// Oid:    oid.T_text,
			// Width:  256,
			// Format: TextFormat,
			// },
		}
		dw.Define(table)
		// dw.Row([]interface{}{29, "John"})
		// dw.Row([]interface{}{13, "Marry"})
		dw.Row([]interface{}{13})

		dw.ParseComplete()

		return nil
	case types.PreparePortal:
		dw := &dataWriter{
			ctx:    ctx,
			client: writer,
		}

		dw.ParseComplete()

		return nil

	default:
		return fmt.Errorf("unknown describe type: %s", typ)
	}
}

func (srv *Server) handleBind(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	bind := pgproto3.Bind{}
	bind.Decode(reader.Msg)

	if bind.DestinationPortal != "" {
		if _, ok := srv.preparedPortals[bind.DestinationPortal]; ok {
			return fmt.Errorf("portal %q already exists", bind.DestinationPortal)
		}
	}

	dw := &dataWriter{
		ctx:    ctx,
		client: writer,
	}

	return dw.BindComplete()
}

func (srv *Server) handleConnClose(ctx context.Context) error {
	if srv.CloseConn == nil {
		return nil
	}

	return srv.CloseConn(ctx)
}

func (srv *Server) handleConnTerminate(ctx context.Context) error {
	if srv.TerminateConn == nil {
		return nil
	}

	return srv.TerminateConn(ctx)
}
