const RaftKVClient = require('./raft-client.js');

// 连接到 3 节点集群
const client = new RaftKVClient([
    { id: 1, addr: '127.0.0.1:50051' },
    { id: 2, addr: '127.0.0.1:50052' },
    { id: 3, addr: '127.0.0.1:50053' },
]);

async function main() {
    console.log('=== Raft KV Client Demo (with auto-redirect) ===\n');

    try {
        // Test 1: Put
        console.log('1. Put("test_key", "hello raft")');
        const putResult = await client.put('test_key', 'hello raft');
        console.log(`   Success: ${putResult.success}\n`);

        // Test 2: Get
        console.log('2. Get("test_key")');
        const getResult = await client.get('test_key');
        console.log(`   Found: ${getResult.found}, Value: "${getResult.value}"\n`);

        // Test 3: Put another
        console.log('3. Put("another_key", "distributed system")');
        const putResult2 = await client.put('another_key', 'distributed system');
        console.log(`   Success: ${putResult2.success}\n`);

        // Test 4: Get from any node
        console.log('4. Get("another_key")');
        const getResult2 = await client.get('another_key');
        console.log(`   Found: ${getResult2.found}, Value: "${getResult2.value}"\n`);

        // Test 5: Delete
        console.log('5. Delete("test_key")');
        const delResult = await client.delete('test_key');
        console.log(`   Success: ${delResult.success}\n`);

        // Test 6: Get deleted key
        console.log('6. Get("test_key") after delete');
        const getResult3 = await client.get('test_key');
        console.log(`   Found: ${getResult3.found}\n`);

        console.log('=== Demo Complete ===');
    } catch (err) {
        console.error('Error:', err);
    } finally {
        client.close();
    }
}

main();
