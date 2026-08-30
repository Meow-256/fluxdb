export interface FluxDBOptions {
    host?: string;
    port?: number;
    password?: string;
    table?: string;
}

export type MeowDBOptions = FluxDBOptions;
export type VeloxDBOptions = FluxDBOptions;

export declare class FluxDB {
    private host;
    private port;
    private password?;
    private defaultTable;
    private socket;
    private buffer;
    private pendingCallbacks;
    constructor(options?: FluxDBOptions);
    connect(): Promise<void>;
    private processBuffer;
    send(command: string): Promise<any>;
    set(key: string, value: any, table?: string): Promise<boolean>;
    get<T = any>(key: string, table?: string): Promise<T | null>;
    jsonSet(key: string, path: string, value: any, table?: string): Promise<boolean>;
    top(path: string, limit?: number, table?: string): Promise<any[]>;
    rank(path: string, key: string, table?: string): Promise<{ uuid: string, rank: number, score: number, total_ranked: number } | null>;
    aroundKey(path: string, key: string, limit?: number, table?: string): Promise<any[]>;
    aroundScore(path: string, score: number, limit?: number, table?: string): Promise<any[]>;
    rankingByScoreRange(path: string, minScore: number, maxScore: number, limit?: number, table?: string): Promise<any[]>;
    rankingByRankRange(path: string, startRank: number, endRank: number, table?: string): Promise<any[]>;
    count(query?: string, table?: string): Promise<number>;
    delete(key: string, table?: string): Promise<boolean>;
    close(): void;
}

export declare const MeowDB: typeof FluxDB;
export declare const VeloxDB: typeof FluxDB;
export default FluxDB;
