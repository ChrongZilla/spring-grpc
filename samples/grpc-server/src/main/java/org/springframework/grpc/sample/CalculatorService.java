package org.springframework.grpc.sample;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.grpc.sample.proto.CalculatorGrpc;
import org.springframework.grpc.sample.proto.NumberRequest;
import org.springframework.grpc.sample.proto.SumReply;
import org.springframework.stereotype.Service;

import io.grpc.stub.StreamObserver;

@Service
public class CalculatorService extends CalculatorGrpc.CalculatorImplBase {

	private static Log log = LogFactory.getLog(CalculatorService.class);

	@Override
	public StreamObserver<NumberRequest> accumulateSum(StreamObserver<SumReply> responseObserver) {
		return new StreamObserver<NumberRequest>() {

			private int total = 0;

			@Override
			public void onNext(NumberRequest request) {
				total += request.getValue();
				log.info("Received " + request.getValue() + ", running total: " + total);

				try {
					Thread.sleep(1000L);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					responseObserver.onError(e);
					return;
				}

				SumReply reply = SumReply.newBuilder().setRunningTotal(total).build();
				responseObserver.onNext(reply);
			}

			@Override
			public void onError(Throwable t) {
				log.warn("Error in accumulateSum", t);
			}

			@Override
			public void onCompleted() {
				responseObserver.onCompleted();
			}
		};
	}

}