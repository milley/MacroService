/**
 * Raft KV 存储集成测试
 *
 * 用法: node integration-test.js <test_type>
 * test_type: basic | consistency | recovery
 */

const RaftKVClient = require('./raft-client.js').RaftKVClient;

// 所有节点地址
const NODES = [
    { id: 1, addr: '127.0.0.1:50051' },
    { id: 2, addr: '127.0.0.1:50052' },
    { id: 3, addr: '127.0.0.1:50053' },
];

// 测试用的 key
const TEST_KEY = 'test_key';
const CONSISTENCY_KEY = 'consistency_key';
const RECOVERY_KEY = 'recovery_key';

// 延迟函数
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// 基础写入测试
async function testBasic() {
    console.log('Running basic write test...');

    const client = new RaftKVClient(NODES);
    let passed = true;

    try {
        // Test Put
        console.log('  Testing Put...');
        const putResult = await client.put(TEST_KEY, 'hello_raft');
        if (!putResult.success) {
            console.log(`  Put failed: ${putResult.error}`);
            passed = false;
        }

        // Test Get
        console.log('  Testing Get...');
        const getResult = await client.get(TEST_KEY);
        if (!getResult.found || getResult.value !== 'hello_raft') {
            console.log(`  Get failed: found=${getResult.found}, value=${getResult.value}`);
            passed = false;
        }

        // Test Delete
        console.log('  Testing Delete...');
        const deleteResult = await client.delete(TEST_KEY);
        if (!deleteResult.success) {
            console.log(`  Delete failed: ${deleteResult.error}`);
            passed = false;
        }

        // Verify deletion
        console.log('  Verifying deletion...');
        const afterDelete = await client.get(TEST_KEY);
        if (afterDelete.found) {
            console.log('  Delete verification failed: key still exists');
            passed = false;
        }

    } catch (e) {
        console.log(`  Error: ${e.message}`);
        passed = false;
    } finally {
        client.close();
    }

    if (passed) {
        console.log('Basic test PASSED');
    } else {
        console.log('Basic test FAILED');
    }

    process.exit(passed ? 0 : 1);
}

// 一致性测试
async function testConsistency() {
    console.log('Running consistency test...');

    const client = new RaftKVClient(NODES);
    let passed = true;

    try {
        // 写入数据（不删除，用于恢复测试验证）
        console.log('  Writing test data...');
        const putResult = await client.put(CONSISTENCY_KEY, 'same_value');
        if (!putResult.success) {
            console.log(`  Put failed: ${putResult.error}`);
            passed = false;
        }

        // 等待数据复制
        await sleep(200);

        // 从每个节点读取，验证一致性
        console.log('  Reading from all nodes...');

        for (const node of NODES) {
            // 创建单独连接到特定节点
            const nodeClient = new RaftKVClient([node]);
            const result = await nodeClient.get(CONSISTENCY_KEY);
            nodeClient.close();

            if (!result.found) {
                console.log(`  Node ${node.id}: key not found`);
                passed = false;
            } else if (result.value !== 'same_value') {
                console.log(`  Node ${node.id}: wrong value '${result.value}'`);
                passed = false;
            } else {
                console.log(`  Node ${node.id}: OK`);
            }
        }

        // 注意：不删除 consistency_key，用于恢复测试验证

    } catch (e) {
        console.log(`  Error: ${e.message}`);
        passed = false;
    } finally {
        client.close();
    }

    if (passed) {
        console.log('Consistency test PASSED');
    } else {
        console.log('Consistency test FAILED');
    }

    process.exit(passed ? 0 : 1);
}

// 恢复测试
async function testRecovery() {
    console.log('Running recovery test...');

    const client = new RaftKVClient(NODES);
    let passed = true;

    try {
        // 验证一致性测试写入的数据是否恢复
        console.log('  Verifying data persisted across restart...');
        const result = await client.get(CONSISTENCY_KEY);

        if (!result.found) {
            console.log('  FAILED: consistency_key not found after restart');
            passed = false;
        } else if (result.value !== 'same_value') {
            console.log(`  FAILED: wrong value '${result.value}', expected 'same_value'`);
            passed = false;
        } else {
            console.log('  OK: Data recovered successfully');
        }

        // 清理测试数据
        console.log('  Cleaning up test data...');
        await client.delete(CONSISTENCY_KEY);

    } catch (e) {
        console.log(`  Error: ${e.message}`);
        passed = false;
    } finally {
        client.close();
    }

    if (passed) {
        console.log('Recovery test PASSED');
    } else {
        console.log('Recovery test FAILED');
    }

    process.exit(passed ? 0 : 1);
}

// 主入口
async function main() {
    const testType = process.argv[2] || 'basic';

    console.log(`Integration test type: ${testType}`);
    console.log('');

    switch (testType) {
        case 'basic':
            await testBasic();
            break;
        case 'consistency':
            await testConsistency();
            break;
        case 'recovery':
            await testRecovery();
            break;
        default:
            console.log(`Unknown test type: ${testType}`);
            console.log('Usage: node integration-test.js <basic|consistency|recovery>');
            process.exit(1);
    }
}

main().catch(e => {
    console.error('Test error:', e);
    process.exit(1);
});
