package rpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"reflect"
	"strings"

	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"golang.org/x/net/context"
)

// NewHTTPProxyServer creates a new HTTP RPC server proxy around an API provider.
func NewHTTPProxyServer(target *url.URL, cors string, handler *Server) *http.Server {
	return &http.Server{
		Handler: &httpConnHijacker{
			corsdomains: strings.Split(cors, ","),
			rpcServer:   handler,
			proxy:       httputil.NewSingleHostReverseProxy(target),
			proxyURL:    target,
		},
	}
}

func writeResult(w ServerCodec, val interface{}) {
	w.Write(val)
}

func writeError(w ServerCodec, err interface{}) {
	glog.V(logger.Info).Infof("Error: %+v\n", err)
	w.Write(err)
}

func writeErrorMsg(w ServerCodec, err error, in *JSONRequest) {
	glog.V(logger.Info).Infof("Error: %v\n", err)
	if in != nil {
		w.Write(JSONErrResponse{in.Version, in.Id, JSONError{-32600, err.Error(), nil}})
		return
	}
	w.Write(err.Error())
}

// sendError writes a formatted JSON-RPC error
func sendError(w http.ResponseWriter, msg string, r *JSONRequest) {
	glog.V(logger.Info).Infof("Error: %s\n", msg)
	if r != nil {
		e := JSONErrResponse{r.Version, r.Id, JSONError{-32600, msg, nil}}
		body, err := json.Marshal(e)
		if err == nil {
			http.Error(w, string(body), http.StatusBadRequest)
			return
		}
	}
	http.Error(w, msg, http.StatusBadRequest)
}

// methodIsDisabled returns true if the request method is not enabled.
func methodIsDisabled(in *JSONRequest) bool {
	for _, m := range proxyDisabledMethods {
		if in.Method == m {
			return true
		}
	}
	return false
}

// parseIncomingRequest gets a JSONRequst struct from the http.Request body, and resets the body so it can be used again.
func parseIncomingRequest(r *http.Request) (*JSONRequest, error) {

	req := &JSONRequest{}
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	// Reset the request body so it can be re-read later on if needed.
	defer func() {
		r.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
	}()

	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		return nil, err
	}

	return req, nil
}

// handleRequest executes the JSONRequest on the local server, returning the result.
func (h *httpConnHijacker) handleRequest(codec ServerCodec, req *JSONRequest) (interface{}, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	return h.doRequest(codec, b)
}

// doRequest executes the request data (a json encoded JSONRequest) on the local server, and returns the result.
func (h *httpConnHijacker) doRequest(codec ServerCodec, data []byte) (interface{}, error) {

	var sReq *serverRequest
	var ok bool
	var svc *service

	request, _, err := parseRequest(json.RawMessage(data))
	if len(request) < 1 {
		glog.V(logger.Debug).Infof("Error parsing request: %v\n", err)
		return request, err
	}

	if svc, ok = h.rpcServer.services[request[0].service]; !ok {
		sReq = &serverRequest{id: request[0].id, err: &methodNotFoundError{request[0].service, request[0].method}}
	}

	if callb, ok := svc.callbacks[request[0].method]; ok {
		sReq = &serverRequest{id: request[0].id, svcname: svc.name, callb: callb}
		if request[0].params != nil && len(callb.argTypes) > 0 {
			if args, err := codec.ParseRequestArguments(callb.argTypes, request[0].params); err == nil {
				sReq.args = args
			} else {
				sReq.err = &invalidParamsError{err.Error()}
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := h.rpcServer.handle(ctx, codec, sReq)

	if svc, ok = h.rpcServer.services[request[0].service]; !ok {
		sReq = &serverRequest{id: request[0].id, err: &methodNotFoundError{request[0].service, request[0].method}}
	}

	if callb, ok := svc.callbacks[request[0].method]; ok {
		sReq = &serverRequest{id: request[0].id, svcname: svc.name, callb: callb}
		if request[0].params != nil && len(callb.argTypes) > 0 {
			if args, err := codec.ParseRequestArguments(callb.argTypes, request[0].params); err == nil {
				sReq.args = args
			} else {
				sReq.err = &invalidParamsError{err.Error()}
			}
		}
	}

    // If the result is an error, we'll also return a error variable with the error message.
	if result != nil && reflect.TypeOf(result) == reflect.TypeOf(&JSONErrResponse{}) {
		return result, errors.New(reflect.ValueOf(result).Elem().FieldByName("Message").String())
	}

	return result, nil
}

// ServeHTTPProxy is the drop-in replacement for ServeHTTP which sends the request to the upstream proxy server.
// It also can check for disabled methods, and re-write some methods as well.
func (h *httpConnHijacker) ServeHTTPProxy(w http.ResponseWriter, req *http.Request) {

    // Make sure the request body has content, otherwise a panic will occur
	if req.Body != nil {

        // Get the data from the incoming request so we can examine and (re-)route it if needed.
		in, err := parseIncomingRequest(req)
		if err != nil {
			sendError(w, err.Error(), nil)
		}

        // Check that the method is not disabled. If it is, return an error.
		if methodIsDisabled(in) {
			sendError(w, fmt.Sprintf("%s is disabled", in.Method), in)
			return
		}

		// eth_sendTransaction => eth_signTransactionLocal => eth_sendRawTransaction
		if in.Method == "eth_sendTransaction" {

            // getCodec here allows this request to be routed to the local server for processing there.
            // The codec here is the local server's method to processing the request.
			codec, err := h.getCodec(w, req)
			if err != nil {
				sendError(w, err.Error(), in)
				return
			}

			// Rewrite the request to an eth_sendTransaction to an eth_signTransactionLocal request.
            // Since eth_signTransactionLocal takes the same parameters, so we can just switch the method.
            // This request will be handled by the local server.
            in.Method = "eth_signTransactionLocal"
			result, err := h.handleRequest(codec, in)
			if err != nil {
				if result != nil {
					writeError(codec, result)
					return
				}
				writeErrorMsg(codec, err, in)
				return
			}

            // Examine the type of the result, and handle accordingly.
			switch reflect.TypeOf(result) {
            // If the result from the local server was an error, return it to the client now.
			case reflect.TypeOf(&JSONErrResponse{}):
				writeError(codec, result)
				return
            // If no error, we have a locally signed transaction, so create a eth_sendRawTransaction request, send it to the proxy, and write the result.
			case reflect.TypeOf(&JSONSuccessResponse{}):
				payload := []interface{}{reflect.ValueOf(&result).Elem().FieldByName("Result").Interface()}
				payloadBytes, _ := json.Marshal(payload)
				rawTx := JSONRequest{"eth_sendRawTransaction", in.Version, in.Id, payloadBytes}
				codec.Write(h.SendExternal(rawTx))
				return
			}

			// Should never get here, as previous results would have to be JSONErrResponse or JSONSuccessResponse, but just in case.
            glog.V(logger.Info).Infof("Unexpected response [%v]: %+v", reflect.TypeOf(result), result)
			writeErrorMsg(codec, errors.New("Unexpected upstream response"), in)
			return

		}
	}

    // If the method is not disabled and not eth_sendTransaction, (or body is nil) we use Go's built-in proxy implementation
    // to send the request and write the results back to the client.
	h.proxy.ServeHTTP(w, req)
	return
}

// SendExternal routes the parameters to the upstream proxy server and returns the results.
func (h *httpConnHijacker) SendExternal(in interface{}) interface{} {
	var resp interface{}
	c, err := h.getExternalClient()
	if err != nil {
		return fmt.Sprintf("Could not prepare request: %v", err)
	}
	if err := c.Send(in); err != nil {
		return fmt.Sprintf("Could not send request: %v", err)
	}
	if err := c.Recv(&resp); err != nil {
		return "Invalid upstream response"
	}
	return resp
}

// getExternalClient gets a new HTTP client for sending requests to the proxy server
func (h *httpConnHijacker) getExternalClient() (Client, error) {
	return NewHTTPClient(h.proxyURL.String())
}

// useProxy returns whether the --proxyproxy flag is enabled.
func (h *httpConnHijacker) useProxy(req *http.Request) bool {
	return h.proxy != nil
}

// getCodec returns a server codec which allows processing of requests locally even if the --rpcproxy flag is enabled
func (h *httpConnHijacker) getCodec(w http.ResponseWriter, req *http.Request) (ServerCodec, error) {

	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("webserver doesn't support hijacking")
	}

	conn, rwbuf, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	return NewJSONCodec(NewHTTPMessageStream(conn, rwbuf, req, h.corsdomains)), nil
}
