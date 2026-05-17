const grpc = require('@grpc/grpc-js');
const path = require('path');

// 加载生成的 proto 文件
const messages = require('./proto/calculator_pb.js');
const services = require('./proto/calculator_grpc_pb.js');

// 创建客户端
const client = new services.CalculatorClient(
    '127.0.0.1:50051',
    grpc.credentials.createInsecure()
);

console.log('=== gRPC Calculator Client Demo ===\n');

// 1. 简单一元调用：Add
function add(a, b) {
    return new Promise((resolve, reject) => {
        const request = new messages.AddRequest();
        request.setA(a);
        request.setB(b);

        client.add(request, (err, response) => {
            if (err) reject(err);
            else resolve(response.getResult());
        });
    });
}

// 2. 服务端流：StreamPrimes
function streamPrimes(limit) {
    return new Promise((resolve) => {
        const request = new messages.PrimesRequest();
        request.setLimit(limit);

        const call = client.streamPrimes(request);
        const primes = [];

        call.on('data', (response) => {
            primes.push(response.getPrime());
        });

        call.on('end', () => {
            resolve(primes);
        });

        call.on('error', (err) => {
            console.error('Stream error:', err);
        });
    });
}

// 3. 客户端流：SumStream
function sumStream(numbers) {
    return new Promise((resolve, reject) => {
        const call = client.sumStream((err, response) => {
            if (err) reject(err);
            else resolve({ sum: response.getSum(), count: response.getCount() });
        });

        numbers.forEach(n => {
            const num = new messages.Number();
            num.setValue(n);
            call.write(num);
        });

        call.end();
    });
}

// 运行演示
async function main() {
    try {
        // 测试 Add
        console.log('1. Unary Call - Add(10, 20)');
        const result = await add(10, 20);
        console.log(`   Result: ${result}\n`);

        // 测试 StreamPrimes
        console.log('2. Server Streaming - Primes < 30');
        const primes = await streamPrimes(30);
        console.log(`   Primes: ${primes.join(', ')}\n`);

        // 测试 SumStream
        console.log('3. Client Streaming - Sum of [1, 2, 3, 4, 5]');
        const { sum, count } = await sumStream([1, 2, 3, 4, 5]);
        console.log(`   Sum: ${sum}, Count: ${count}\n`);

        console.log('=== Demo Complete ===');
    } catch (err) {
        console.error('Error:', err);
    } finally {
        grpc.closeClient(client);
    }
}

main();
