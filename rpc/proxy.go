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

func methodIsDisabled(in *JSONRequest) bool {
	for _, m := range proxyDisabledMethods {
		if in.Method == m {
			return true
		}
	}
	return false
}

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

func (h *httpConnHijacker) handleRequest(codec ServerCodec, req *JSONRequest) (interface{}, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	return h.doRequest(codec, b)
}

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

	if reflect.TypeOf(result) == reflect.TypeOf(JSONErrResponse{}) {
		return result, errors.New(reflect.ValueOf(&result).Elem().FieldByName("Message").String())
	}

	return result, nil
}

func (h *httpConnHijacker) ServeHTTPProxy(w http.ResponseWriter, req *http.Request) {

	if req.Body != nil {

		in, err := parseIncomingRequest(req)
		if err != nil {
			sendError(w, err.Error(), nil)
		}

		if methodIsDisabled(in) {
			sendError(w, fmt.Sprintf("%s is disabled", in.Method), in)
			return
		}

		// eth_sendTransaction => eth_signTransactionLocal => eth_sendRawTransaction
		if in.Method == "eth_sendTransaction" {

			codec, err := h.getCodec(w, req)
			if err != nil {
				sendError(w, err.Error(), in)
				return
			}

			// Rewrite the request to an eth_signTransaction request. It takes the same parameters, so we can just copy.
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

			switch reflect.TypeOf(result) {
			case reflect.TypeOf(&JSONErrResponse{}):
				writeError(codec, result)
				return
			case reflect.TypeOf(&JSONSuccessResponse{}):
				// If no error, we have a locally signed transaction, so create a eth_sendRawTransaction request, send it to the proxy, and write the result.
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

	h.proxy.ServeHTTP(w, req)
	return
}

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

func (h *httpConnHijacker) getExternalClient() (Client, error) {
	return NewHTTPClient(h.proxyURL.String())
}

func (h *httpConnHijacker) useProxy(req *http.Request) bool {
	return h.proxy != nil
}

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
