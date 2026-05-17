const grpc = require('@grpc/grpc-js');
const path = require('path');

// 加载生成的 proto 文件
const kvMessages = require('./proto/kv_pb.js');
const kvServices = require('./proto/kv_grpc_pb.js');

// 创建客户端
const client = new kvServices.KVServiceClient(
    '127.0.0.1:50051',
    grpc.credentials.createInsecure()
);

console.log('=== KV Store Client Demo ===\n');

// Put 操作
function put(key, value) {
    return new Promise((resolve, reject) => {
        const request = new kvMessages.PutRequest();
        request.setKey(key);
        request.setValue(Buffer.from(value));

        client.put(request, (err, response) => {
            if (err) reject(err);
            else resolve(response.getSuccess());
        });
    });
}

// Get 操作
function get(key) {
    return new Promise((resolve, reject) => {
        const request = new kvMessages.GetRequest();
        request.setKey(key);

        client.get(request, (err, response) => {
            if (err) reject(err);
            else {
                const valueBytes = response.getValue();
                const value = valueBytes.length > 0 ? Buffer.from(valueBytes).toString('utf8') : '';
                resolve({
                    found: response.getFound(),
                    value: value,
                    error: response.getError()
                });
            }
        });
    });
}

// Delete 操作
function del(key) {
    return new Promise((resolve, reject) => {
        const request = new kvMessages.DeleteRequest();
        request.setKey(key);

        client.delete(request, (err, response) => {
            if (err) reject(err);
            else resolve(response.getSuccess());
        });
    });
}

// 运行演示
async function main() {
    try {
        // Put
        console.log('1. Put("name", "Raft KV Store")');
        await put('name', 'Raft KV Store');
        console.log('   Success!\n');

        // Get
        console.log('2. Get("name")');
        let result = await get('name');
        console.log(`   Found: ${result.found}, Value: "${result.value}"\n`);

        // Put another
        console.log('3. Put("count", "42")');
        await put('count', '42');
        console.log('   Success!\n');

        // Get
        console.log('4. Get("count")');
        result = await get('count');
        console.log(`   Found: ${result.found}, Value: "${result.value}"\n`);

        // Delete
        console.log('5. Delete("count")');
        await del('count');
        console.log('   Success!\n');

        // Get deleted key
        console.log('6. Get("count") after delete');
        result = await get('count');
        console.log(`   Found: ${result.found}\n`);

        console.log('=== Demo Complete ===');
    } catch (err) {
        console.error('Error:', err);
    } finally {
        grpc.closeClient(client);
    }
}

main();
