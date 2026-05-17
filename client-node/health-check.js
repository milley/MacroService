/**
 * 简单的健康检查脚本
 * 用于检测 Leader 是否就绪
 */

const RaftKVClient = require('./raft-client.js').RaftKVClient;

const NODES = [
    { id: 1, addr: '127.0.0.1:50051' },
    { id: 2, addr: '127.0.0.1:50052' },
    { id: 3, addr: '127.0.0.1:50053' },
];

async function check() {
    const client = new RaftKVClient(NODES);

    try {
        // 尝试写入一个测试 key
        const result = await client.put('__health_check__', 'ok');
        client.close();

        if (result.success) {
            process.exit(0);
        } else {
            process.exit(1);
        }
    } catch (e) {
        client.close();
        process.exit(1);
    }
}

check();
