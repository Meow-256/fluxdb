"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.FluxDB = exports.VeloxDB = exports.MeowDB = void 0;
const net = __importStar(require("net"));

class FluxDB {
    constructor(options = {}) {
        this.socket = null;
        this.buffer = '';
        this.pendingCallbacks = [];
        this.host = options.host || '127.0.0.1';
        this.port = options.port || 7379;
        this.password = options.password;
        this.defaultTable = options.table || 'players';
    }
    async connect() {
        return new Promise((resolve, reject) => {
            this.socket = net.createConnection({ host: this.host, port: this.port }, async () => {
                var _a;
                (_a = this.socket) === null || _a === void 0 ? void 0 : _a.setNoDelay(true);
                if (this.password) {
                    try {
                        await this.send(`AUTH ${this.password}`);
                    }
                    catch (e) {
                        return reject(e);
                    }
                }
                resolve();
            });
            this.socket.on('data', (chunk) => {
                this.buffer += chunk.toString('utf-8');
                this.processBuffer();
            });
            this.socket.on('error', (err) => {
                if (this.pendingCallbacks.length > 0) {
                    const cb = this.pendingCallbacks.shift();
                    cb === null || cb === void 0 ? void 0 : cb.reject(err);
                }
            });
        });
    }
    processBuffer() {
        while (this.buffer.length > 0 && this.pendingCallbacks.length > 0) {
            if (this.buffer.startsWith('+') || this.buffer.startsWith(':') || this.buffer.startsWith('-')) {
                const idx = this.buffer.indexOf('\r\n');
                if (idx === -1)
                    break;
                const line = this.buffer.slice(0, idx);
                this.buffer = this.buffer.slice(idx + 2);
                const cb = this.pendingCallbacks.shift();
                if (line.startsWith('-ERR')) {
                    cb === null || cb === void 0 ? void 0 : cb.reject(new Error(line.slice(5)));
                }
                else if (line.startsWith('+')) {
                    cb === null || cb === void 0 ? void 0 : cb.resolve(line.slice(1));
                }
                else if (line.startsWith(':')) {
                    cb === null || cb === void 0 ? void 0 : cb.resolve(parseInt(line.slice(1), 10));
                }
            }
            else if (this.buffer.startsWith('$')) {
                const idx = this.buffer.indexOf('\r\n');
                if (idx === -1)
                    break;
                const lenStr = this.buffer.slice(1, idx);
                const len = parseInt(lenStr, 10);
                if (len === -1) {
                    this.buffer = this.buffer.slice(idx + 2);
                    const cb = this.pendingCallbacks.shift();
                    cb === null || cb === void 0 ? void 0 : cb.resolve(null);
                }
                else {
                    if (this.buffer.length < idx + 2 + len + 2)
                        break;
                    const payload = this.buffer.slice(idx + 2, idx + 2 + len);
                    this.buffer = this.buffer.slice(idx + 2 + len + 2);
                    const cb = this.pendingCallbacks.shift();
                    cb === null || cb === void 0 ? void 0 : cb.resolve(payload);
                }
            }
            else {
                const idx = this.buffer.indexOf('\r\n');
                if (idx === -1)
                    break;
                const line = this.buffer.slice(0, idx);
                this.buffer = this.buffer.slice(idx + 2);
                const cb = this.pendingCallbacks.shift();
                cb === null || cb === void 0 ? void 0 : cb.resolve(line);
            }
        }
    }
    async send(command) {
        return new Promise((resolve, reject) => {
            if (!this.socket) {
                return reject(new Error('Socket not connected'));
            }
            this.pendingCallbacks.push({ resolve, reject });
            this.socket.write(command.endsWith('\r\n') ? command : command + '\r\n');
        });
    }
    async set(key, value, table) {
        const t = table || this.defaultTable;
        const valStr = typeof value === 'object' ? JSON.stringify(value) : String(value);
        const res = await this.send(`SET ${t} ${key} ${valStr}`);
        return res === 'OK';
    }
    async get(key, table) {
        const t = table || this.defaultTable;
        const raw = await this.send(`GET ${t} ${key}`);
        if (raw === null)
            return null;
        try {
            return JSON.parse(raw);
        }
        catch {
            return raw;
        }
    }
    async jsonSet(key, path, value, table) {
        const t = table || this.defaultTable;
        const valStr = JSON.stringify(value);
        const res = await this.send(`JSON.SET ${t} ${key} ${path} ${valStr}`);
        return res === 'OK';
    }
    async top(path, limit = 10, table) {
        const t = table || this.defaultTable;
        const raw = await this.send(`TOP ${t} ${path} ${limit}`);
        return raw ? JSON.parse(raw) : [];
    }
    async rank(path, key, table) {
        const t = table || this.defaultTable;
        const raw = await this.send(`RANK ${t} ${path} ${key}`);
        return raw ? JSON.parse(raw) : null;
    }
    async aroundKey(path, key, limit = 10, table) {
        const t = table || this.defaultTable;
        const raw = await this.send(`RANK.KEY ${t} ${path} ${key} ${limit}`);
        return raw ? JSON.parse(raw) : [];
    }
    async aroundScore(path, score, limit = 10, table) {
        const t = table || this.defaultTable;
        const raw = await this.send(`RANK.SCORE ${t} ${path} ${score} ${limit}`);
        return raw ? JSON.parse(raw) : [];
    }
    async rankingByScoreRange(path, minScore, maxScore, limit = 50, table) {
        const t = table || this.defaultTable;
        const raw = await this.send(`RANK.RANGE_SCORE ${t} ${path} ${minScore} ${maxScore} ${limit}`);
        return raw ? JSON.parse(raw) : [];
    }
    async rankingByRankRange(path, startRank, endRank, table) {
        const t = table || this.defaultTable;
        const raw = await this.send(`RANK.RANGE ${t} ${path} ${startRank} ${endRank}`);
        return raw ? JSON.parse(raw) : [];
    }
    async count(query, table) {
        const t = table || this.defaultTable;
        const cmd = query ? `COUNT ${t} ${query}` : `COUNT ${t}`;
        return await this.send(cmd);
    }
    async delete(key, table) {
        const t = table || this.defaultTable;
        const res = await this.send(`DEL ${t} ${key}`);
        return res === 1 || res === '1';
    }
    close() {
        if (this.socket) {
            this.socket.destroy();
            this.socket = null;
        }
    }
}
exports.FluxDB = FluxDB;
exports.MeowDB = FluxDB;
exports.VeloxDB = FluxDB;
exports.default = FluxDB;
