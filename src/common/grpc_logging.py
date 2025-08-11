import time
import logging
import grpc

log = logging.getLogger("grpc")

class ServerLoggingInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method  # e.g. /energy.EnergyAnalyzer/AnalyzeData
        handler = continuation(handler_call_details)

        # Wrapping unary-unary.
        if handler and handler.unary_unary:
            def inner(request, context):
                start = time.time()
                code = grpc.StatusCode.OK
                try:
                    resp = handler.unary_unary(request, context)
                    # context._state.code is None for OK
                    return resp
                except Exception:
                    code = grpc.StatusCode.UNKNOWN
                    log.exception("rpc error method=%s", method)
                    raise
                finally:
                    dur_ms = int((time.time() - start) * 1000)
                    # if handler set a non-OK status, prefer that
                    try:
                        code = context._state.code or code
                    except Exception:
                        pass
                    log.info("rpc method=%s code=%s dur_ms=%d", method, code.name, dur_ms)

            return grpc.unary_unary_rpc_method_handler(
                inner,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        return handler
