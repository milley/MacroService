use tonic::{transport::Server, Request, Response, Status};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

pub mod calculator {
    tonic::include_proto!("calculator");
}

use calculator::{
    calculator_server::{Calculator, CalculatorServer},
    AddRequest, AddResponse, Number, PrimeResponse, PrimesRequest, SumResponse,
};

#[derive(Debug, Default)]
pub struct MyCalculator {}

fn is_prime(n: i32) -> bool {
    if n < 2 {
        return false;
    }
    for i in 2..=((n as f64).sqrt() as i32) {
        if n % i == 0 {
            return false;
        }
    }
    true
}

#[tonic::async_trait]
impl Calculator for MyCalculator {
    async fn add(&self, request: Request<AddRequest>) -> Result<Response<AddResponse>, Status> {
        let req = request.into_inner();
        println!("[Add] {} + {}", req.a, req.b);

        Ok(Response::new(AddResponse {
            result: req.a + req.b,
        }))
    }

    type StreamPrimesStream = ReceiverStream<Result<PrimeResponse, Status>>;

    async fn stream_primes(
        &self,
        request: Request<PrimesRequest>,
    ) -> Result<Response<Self::StreamPrimesStream>, Status> {
        let limit = request.into_inner().limit;
        println!("[StreamPrimes] Generating primes < {}", limit);

        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            for n in 2..limit {
                if is_prime(n) {
                    tx.send(Ok(PrimeResponse { prime: n }))
                        .await
                        .unwrap();
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn sum_stream(
        &self,
        request: Request<tonic::Streaming<Number>>,
    ) -> Result<Response<SumResponse>, Status> {
        let mut stream = request.into_inner();
        let mut sum = 0i32;
        let mut count = 0i32;

        println!("[SumStream] Receiving numbers...");

        while let Some(number) = stream.next().await {
            let n = number?.value;
            sum += n;
            count += 1;
            println!("  received: {}, running sum: {}", n, sum);
        }

        println!("[SumStream] Done. Total: {} from {} numbers", sum, count);

        Ok(Response::new(SumResponse { sum, count }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;
    let calculator = MyCalculator::default();

    println!("Calculator gRPC server listening on {}", addr);

    Server::builder()
        .add_service(CalculatorServer::new(calculator))
        .serve(addr)
        .await?;

    Ok(())
}
