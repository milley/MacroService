const grpc = require('@grpc/grpc-js');
const kvMessages = require('./proto/kv_pb.js');
const kvServices = require('./proto/kv_grpc_pb.js');

class RaftKVClient {
    constructor(nodeAddresses) {
        // nodeAddresses: [{id: 1, addr: '127.0.0.1:50051'}, ...]
        this.nodes = nodeAddresses;
        this.clients = {};
        this.leaderHint = null;

        for (const node of nodeAddresses) {
            this.clients[node.id] = new kvServices.KVServiceClient(
                node.addr,
                grpc.credentials.createInsecure()
            );
        }
    }

    // 获取客户端
    getClient(nodeId) {
        return this.clients[nodeId];
    }

    // 尝试任意节点
    getAnyClient() {
        if (this.leaderHint && this.clients[this.leaderHint]) {
            return { id: this.leaderHint, client: this.clients[this.leaderHint] };
        }
        const first = this.nodes[0];
        return { id: first.id, client: this.clients[first.id] };
    }

    // Get 操作（自动重定向到 Leader）
    async get(key) {
        for (let attempt = 0; attempt < 3; attempt++) {
            const { id, client } = this.getAnyClient();

            const result = await new Promise((resolve) => {
                const request = new kvMessages.GetRequest();
                request.setKey(key);

                client.get(request, (err, response) => {
                    if (err) {
                        resolve({ found: false, error: err.message, leaderHint: 0, value: null });
                    } else {
                        resolve({
                            found: response.getFound(),
                            value: response.getFound() ? Buffer.from(response.getValue()).toString('utf8') : null,
                            error: response.getError(),
                            leaderHint: response.getLeaderHint(),
                        });
                    }
                });
            });

            if (result.found || !result.error) {
                return { found: result.found, value: result.value };
            }

            if (result.error === 'not leader' && result.leaderHint > 0) {
                this.leaderHint = result.leaderHint;
                continue;
            }

            return { found: false, value: null };
        }

        return { found: false, value: null };
    }

    // Put 操作（自动重定向）
    async put(key, value) {
        const valueBytes = Buffer.from(value, 'utf8');

        for (let attempt = 0; attempt < 3; attempt++) {
            const { id, client } = this.getAnyClient();

            const result = await new Promise((resolve) => {
                const request = new kvMessages.PutRequest();
                request.setKey(key);
                request.setValue(valueBytes);

                client.put(request, (err, response) => {
                    if (err) {
                        resolve({ success: false, error: err.message, leaderHint: 0 });
                    } else {
                        resolve({
                            success: response.getSuccess(),
                            error: response.getError(),
                            leaderHint: response.getLeaderHint(),
                        });
                    }
                });
            });

            if (result.success) {
                return { success: true };
            }

            if (result.error === 'not leader' && result.leaderHint > 0) {
                console.log(`  Redirecting to leader node ${result.leaderHint}`);
                this.leaderHint = result.leaderHint;
                continue;
            }

            return { success: false, error: result.error };
        }

        return { success: false, error: 'Failed after retries' };
    }

    // Delete 操作（自动重定向）
    async delete(key) {
        for (let attempt = 0; attempt < 3; attempt++) {
            const { id, client } = this.getAnyClient();

            const result = await new Promise((resolve) => {
                const request = new kvMessages.DeleteRequest();
                request.setKey(key);

                client.delete(request, (err, response) => {
                    if (err) {
                        resolve({ success: false, error: err.message, leaderHint: 0 });
                    } else {
                        resolve({
                            success: response.getSuccess(),
                            error: response.getError(),
                            leaderHint: response.getLeaderHint(),
                        });
                    }
                });
            });

            if (result.success) {
                return { success: true };
            }

            if (result.error === 'not leader' && result.leaderHint > 0) {
                console.log(`  Redirecting to leader node ${result.leaderHint}`);
                this.leaderHint = result.leaderHint;
                continue;
            }

            return { success: false, error: result.error };
        }

        return { success: false, error: 'Failed after retries' };
    }

    close() {
        for (const id in this.clients) {
            grpc.closeClient(this.clients[id]);
        }
    }
}

module.exports = RaftKVClient;
module.exports.RaftKVClient = RaftKVClient;
