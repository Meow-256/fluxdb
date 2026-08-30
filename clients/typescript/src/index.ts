import * as net from 'net';

export interface FluxDBOptions {
  host?: string;
  port?: number;
  password?: string;
  table?: string;
}

export class FluxDB {
  private host: string;
  private port: number;
  private password?: string;
  private defaultTable: string;
  private socket: net.Socket | null = null;
  private buffer: string = '';
  private pendingCallbacks: Array<{
    resolve: (val: any) => void;
    reject: (err: Error) => void;
  }> = [];

  constructor(options: FluxDBOptions = {}) {
    this.host = options.host || '127.0.0.1';
    this.port = options.port || 7379;
    this.password = options.password;
    this.defaultTable = options.table || 'players';
  }

  public async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection({ host: this.host, port: this.port }, async () => {
        this.socket?.setNoDelay(true);
        if (this.password) {
          try {
            await this.send(`AUTH ${this.password}`);
          } catch (e) {
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
          cb?.reject(err);
        }
      });
    });
  }

  private processBuffer() {
    while (this.buffer.length > 0 && this.pendingCallbacks.length > 0) {
      if (this.buffer.startsWith('+') || this.buffer.startsWith(':') || this.buffer.startsWith('-')) {
        const idx = this.buffer.indexOf('\r\n');
        if (idx === -1) break;
        const line = this.buffer.slice(0, idx);
        this.buffer = this.buffer.slice(idx + 2);
        const cb = this.pendingCallbacks.shift();
        if (line.startsWith('-ERR')) {
          cb?.reject(new Error(line.slice(5)));
        } else if (line.startsWith('+')) {
          cb?.resolve(line.slice(1));
        } else if (line.startsWith(':')) {
          cb?.resolve(parseInt(line.slice(1), 10));
        }
      } else if (this.buffer.startsWith('$')) {
        const idx = this.buffer.indexOf('\r\n');
        if (idx === -1) break;
        const lenStr = this.buffer.slice(1, idx);
        const len = parseInt(lenStr, 10);
        if (len === -1) {
          this.buffer = this.buffer.slice(idx + 2);
          const cb = this.pendingCallbacks.shift();
          cb?.resolve(null);
        } else {
          if (this.buffer.length < idx + 2 + len + 2) break;
          const payload = this.buffer.slice(idx + 2, idx + 2 + len);
          this.buffer = this.buffer.slice(idx + 2 + len + 2);
          const cb = this.pendingCallbacks.shift();
          cb?.resolve(payload);
        }
      } else {
        const idx = this.buffer.indexOf('\r\n');
        if (idx === -1) break;
        const line = this.buffer.slice(0, idx);
        this.buffer = this.buffer.slice(idx + 2);
        const cb = this.pendingCallbacks.shift();
        cb?.resolve(line);
      }
    }
  }

  public async send(command: string): Promise<any> {
    return new Promise((resolve, reject) => {
      if (!this.socket) {
        return reject(new Error('Socket not connected'));
      }
      this.pendingCallbacks.push({ resolve, reject });
      this.socket.write(command.endsWith('\r\n') ? command : command + '\r\n');
    });
  }

  public async set(key: string, value: any, table?: string): Promise<boolean> {
    const t = table || this.defaultTable;
    const valStr = typeof value === 'object' ? JSON.stringify(value) : String(value);
    const res = await this.send(`SET ${t} ${key} ${valStr}`);
    return res === 'OK';
  }

  public async get<T = any>(key: string, table?: string): Promise<T | null> {
    const t = table || this.defaultTable;
    const raw = await this.send(`GET ${t} ${key}`);
    if (raw === null) return null;
    try {
      return JSON.parse(raw);
    } catch {
      return raw as any;
    }
  }

  public async jsonSet(key: string, path: string, value: any, table?: string): Promise<boolean> {
    const t = table || this.defaultTable;
    const valStr = JSON.stringify(value);
    const res = await this.send(`JSON.SET ${t} ${key} ${path} ${valStr}`);
    return res === 'OK';
  }

  public async top(path: string, limit: number = 10, table?: string): Promise<any[]> {
    const t = table || this.defaultTable;
    const raw = await this.send(`TOP ${t} ${path} ${limit}`);
    return raw ? JSON.parse(raw) : [];
  }

  public async rank(path: string, key: string, table?: string): Promise<{ uuid: string, rank: number, score: number, total_ranked: number } | null> {
    const t = table || this.defaultTable;
    const raw = await this.send(`RANK ${t} ${path} ${key}`);
    return raw ? JSON.parse(raw) : null;
  }

  public async aroundKey(path: string, key: string, limit: number = 10, table?: string): Promise<any[]> {
    const t = table || this.defaultTable;
    const raw = await this.send(`RANK.KEY ${t} ${path} ${key} ${limit}`);
    return raw ? JSON.parse(raw) : [];
  }

  public async aroundScore(path: string, score: number, limit: number = 10, table?: string): Promise<any[]> {
    const t = table || this.defaultTable;
    const raw = await this.send(`RANK.SCORE ${t} ${path} ${score} ${limit}`);
    return raw ? JSON.parse(raw) : [];
  }

  public async rankingByScoreRange(path: string, minScore: number, maxScore: number, limit: number = 50, table?: string): Promise<any[]> {
    const t = table || this.defaultTable;
    const raw = await this.send(`RANK.RANGE_SCORE ${t} ${path} ${minScore} ${maxScore} ${limit}`);
    return raw ? JSON.parse(raw) : [];
  }

  public async rankingByRankRange(path: string, startRank: number, endRank: number, table?: string): Promise<any[]> {
    const t = table || this.defaultTable;
    const raw = await this.send(`RANK.RANGE ${t} ${path} ${startRank} ${endRank}`);
    return raw ? JSON.parse(raw) : [];
  }

  public async count(query?: string, table?: string): Promise<number> {
    const t = table || this.defaultTable;
    const cmd = query ? `COUNT ${t} ${query}` : `COUNT ${t}`;
    return await this.send(cmd);
  }

  public async delete(key: string, table?: string): Promise<boolean> {
    const t = table || this.defaultTable;
    const res = await this.send(`DEL ${t} ${key}`);
    return res === 1 || res === '1';
  }

  public close() {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
  }
}

export const MeowDB = FluxDB;
export const VeloxDB = FluxDB;
export type MeowDBOptions = FluxDBOptions;
export type VeloxDBOptions = FluxDBOptions;
export default FluxDB;
